"""Simple ETL: load local CSV files into NeonDB schema polymarket.

Usage:
  python load_csv_to_neon.py

It reads the environment variable NEON_CONN (or DATABASE_URL) from a .env file
located next to this script (same approach as main.py). It expects these CSV files
at the repository root: events_*.csv, markets_*.csv, series_*.csv, tags_*.csv

This is a convenience loader for development; for production the pipeline should
use Delta Lake -> COPY from S3 or a proper bulk loader.
"""
import os
from pathlib import Path
import logging
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
# Prefer .env in the same folder as this script, fallback to repo root
DOTENV_CANDIDATES = [BASE_DIR / '.env', BASE_DIR.parent / '.env']
loaded_path = None
for p in DOTENV_CANDIDATES:
    if p.exists():
        load_dotenv(p)
        loaded_path = p
        break

NEON_CONN = os.getenv('NEON_CONN') or os.getenv('DATABASE_URL')
if NEON_CONN:
    NEON_CONN = NEON_CONN.strip().strip('"').strip("'")

if not NEON_CONN:
    checked = ', '.join(str(p) for p in DOTENV_CANDIDATES)
    raise RuntimeError(
        f"NEON_CONN or DATABASE_URL not found. Checked: {checked}"
    )

engine = create_engine(NEON_CONN)

CSV_MAP = {
    'events': 'events_eeuu_iran_clean.csv',
    'markets': 'markets_eeuu_iran_clean.csv',
    'series': 'series_eeuu_iran_clean.csv',
    'tags': 'tags_eeuu_iran_clean.csv',
}

# Map plural csv keys to singular DB dimension table names used in DDL
TABLE_NAME_MAP = {
    'events': 'dim_event',
    'markets': 'dim_market',
    'series': 'dim_series',
    'tags': 'dim_tag',
}


def load_csv_to_table(csv_path: Path, table_name: str, schema: str = 'polymarket'):
    logger.info('Loading %s -> %s.%s', csv_path.name, schema, table_name)
    df = pd.read_csv(csv_path)
    # Basic normalization: strip column names
    df.columns = [c.strip() for c in df.columns]
    # Convert obvious date columns
    for c in df.columns:
        if 'ts' in c.lower() or 'date' in c.lower() or 'time' in c.lower():
            try:
                df[c] = pd.to_datetime(df[c], utc=True)
            except Exception:
                pass
    # Write to SQL (replace existing)
    df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
    logger.info('Finished loading %s rows into %s.%s', len(df), schema, table_name)


def create_schema_if_missing():
    with engine.begin() as conn:
        conn.execute(text('create schema if not exists polymarket'))


def main():
    create_schema_if_missing()
    # CSV files live at the repository root (one level above this script)
    REPO_ROOT = BASE_DIR.parent
    for key, filename in CSV_MAP.items():
        path = REPO_ROOT / filename
        if not path.exists():
            logger.warning('CSV not found: %s (skipping)', path)
            continue
        table_name = TABLE_NAME_MAP.get(key, f'dim_{key}')
        load_csv_to_table(path, table_name)

    # Optionally, create fact table from joined data (simple example):
    try:
        # Apply DDL which contains clean dims and clean fact definition
        ddl_text = open(Path(__file__).parent / 'ddl.sql').read()
        # execute individual statements so a single failing DROP doesn't abort the run
        with engine.begin() as conn:
            for stmt in ddl_text.split(';'):
                s = stmt.strip()
                if not s:
                    continue
                try:
                    conn.execute(text(s))
                except Exception as e:
                    logger.warning('DDL statement failed, continuing: %s', e)
            logger.info('Attempted to apply DDL (individual statements)')

        # Read raw tables written earlier into pandas for normalization
        df_market_raw = pd.read_sql_table('dim_market', con=engine, schema='polymarket')
        df_event_raw = pd.read_sql_table('dim_event', con=engine, schema='polymarket')
        # series and tags may be empty
        try:
            df_series_raw = pd.read_sql_table('dim_series', con=engine, schema='polymarket')
        except Exception:
            df_series_raw = pd.DataFrame()
        try:
            df_tag_raw = pd.read_sql_table('dim_tag', con=engine, schema='polymarket')
        except Exception:
            df_tag_raw = pd.DataFrame()

        # helper to parse JSON-like cells and extract first id
        def parse_first_id(cell):
            if cell is None or (isinstance(cell, float) and pd.isna(cell)):
                return None
            if isinstance(cell, (list, dict)):
                if isinstance(cell, list) and cell:
                    v = cell[0]
                    if isinstance(v, dict) and 'id' in v:
                        return int(v['id'])
                    if isinstance(v, (int, float)):
                        return int(v)
                return None
            if isinstance(cell, str):
                s = cell.strip()
                try:
                    import json
                    if s.startswith('[') or s.startswith('{'):
                        obj = json.loads(s)
                        if isinstance(obj, list) and obj:
                            v = obj[0]
                            if isinstance(v, dict) and 'id' in v:
                                return int(v['id'])
                            if isinstance(v, (int, float)):
                                return int(v)
                        if isinstance(obj, dict) and 'id' in obj:
                            return int(obj['id'])
                except Exception:
                    pass
                import re
                m = re.search(r"(\d{3,})", s)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return None
            return None

        # Build cleaned event dim
        df_event_clean = pd.DataFrame({
            'event_id': pd.to_numeric(df_event_raw.get('id') if 'id' in df_event_raw.columns else df_event_raw.get('event_id'), errors='coerce'),
            'title': df_event_raw.get('title') if 'title' in df_event_raw.columns else df_event_raw.get('name'),
            'category': df_event_raw.get('category') if 'category' in df_event_raw.columns else None,
            'end_ts': pd.to_datetime(df_event_raw.get('endDate') if 'endDate' in df_event_raw.columns else df_event_raw.get('end_ts'), errors='coerce', utc=True)
        }).dropna(subset=['event_id']).drop_duplicates(subset=['event_id'])
        if len(df_event_clean):
            df_event_clean['event_id'] = df_event_clean['event_id'].astype('int64')

        # Build cleaned tag dim. If tags CSV is empty, extract tags from events payloads.
        if not df_tag_raw.empty:
            df_tag_clean = pd.DataFrame({
                'tag_id': pd.to_numeric(df_tag_raw.get('id'), errors='coerce'),
                'name': df_tag_raw.get('label') if 'label' in df_tag_raw.columns else df_tag_raw.get('name'),
                'event_id': pd.to_numeric(df_tag_raw.get('event_id'), errors='coerce') if 'event_id' in df_tag_raw.columns else None
            }).dropna(subset=['tag_id']).drop_duplicates(subset=['tag_id'])
            if len(df_tag_clean):
                df_tag_clean['tag_id'] = df_tag_clean['tag_id'].astype('int64')
        else:
            # try to parse tags from the events file (events have a 'tags' JSON column)
            import json
            rows = []
            for _, ev in df_event_raw.iterrows():
                # determine event id
                ev_id = None
                if 'id' in df_event_raw.columns:
                    try:
                        ev_id = int(ev.get('id'))
                    except Exception:
                        ev_id = None
                elif 'event_id' in df_event_raw.columns:
                    try:
                        ev_id = int(ev.get('event_id'))
                    except Exception:
                        ev_id = None

                tags_cell = ev.get('tags') if 'tags' in df_event_raw.columns else None
                if tags_cell is None or (isinstance(tags_cell, float) and pd.isna(tags_cell)):
                    continue
                try:
                    if isinstance(tags_cell, str) and tags_cell.strip().startswith(('[', '{')):
                        parsed = json.loads(tags_cell)
                    else:
                        # not JSON - skip
                        continue
                    if isinstance(parsed, list):
                        for t in parsed:
                            if isinstance(t, dict) and 'id' in t:
                                try:
                                    tid = int(t.get('id'))
                                except Exception:
                                    continue
                                name = t.get('label') or t.get('name')
                                rows.append({'tag_id': tid, 'name': name, 'event_id': int(ev_id) if ev_id is not None else None})
                except Exception:
                    continue

            if rows:
                df_tag_clean = pd.DataFrame(rows).dropna(subset=['tag_id']).drop_duplicates(subset=['tag_id','event_id'])
                df_tag_clean['tag_id'] = df_tag_clean['tag_id'].astype('int64')
            else:
                df_tag_clean = pd.DataFrame(columns=['tag_id', 'name', 'event_id'])

        # Build cleaned series dim
        if not df_series_raw.empty:
            df_series_clean = pd.DataFrame({
                'series_id': pd.to_numeric(df_series_raw.get('id'), errors='coerce'),
                'market_id': None,
                'title': df_series_raw.get('title') if 'title' in df_series_raw.columns else None,
                'recorded_ts': pd.to_datetime(df_series_raw.get('recorded_ts') if 'recorded_ts' in df_series_raw.columns else None, errors='coerce', utc=True),
                'probability': pd.to_numeric(df_series_raw.get('probability') if 'probability' in df_series_raw.columns else None, errors='coerce')
            }).dropna(subset=['series_id']).drop_duplicates(subset=['series_id'])
            if len(df_series_clean):
                df_series_clean['series_id'] = df_series_clean['series_id'].astype('int64')
        else:
            df_series_clean = pd.DataFrame(columns=['series_id','market_id','title','recorded_ts','probability'])

        # Build cleaned market dim
        df_market_clean = pd.DataFrame({
            'market_id': pd.to_numeric(df_market_raw.get('id') if 'id' in df_market_raw.columns else df_market_raw.get('market_id'), errors='coerce'),
            'question': df_market_raw.get('question') if 'question' in df_market_raw.columns else df_market_raw.get('title'),
            'category': df_market_raw.get('category') if 'category' in df_market_raw.columns else None,
            'active': df_market_raw.get('active') if 'active' in df_market_raw.columns else False,
            'closed': df_market_raw.get('closed') if 'closed' in df_market_raw.columns else False,
            'archived': df_market_raw.get('archived') if 'archived' in df_market_raw.columns else False,
            'liquidity': pd.to_numeric(df_market_raw.get('liquidityNum') if 'liquidityNum' in df_market_raw.columns else df_market_raw.get('liquidity'), errors='coerce'),
            'volume': pd.to_numeric(df_market_raw.get('volumeNum') if 'volumeNum' in df_market_raw.columns else df_market_raw.get('volume'), errors='coerce'),
            'event_id': None,
            'end_ts': pd.to_datetime(df_market_raw.get('endDate') if 'endDate' in df_market_raw.columns else df_market_raw.get('end_ts'), errors='coerce', utc=True)
        })

        # try to extract event_id from 'events' column if present
        if 'events' in df_market_raw.columns:
            parsed = df_market_raw['events'].apply(parse_first_id)
            df_market_clean['event_id'] = parsed

        # else, use explicit event_id column if exists
        if 'event_id' in df_market_raw.columns:
            df_market_clean['event_id'] = df_market_clean['event_id'].fillna(pd.to_numeric(df_market_raw.get('event_id'), errors='coerce'))

        df_market_clean = df_market_clean.dropna(subset=['market_id']).drop_duplicates(subset=['market_id'])
        df_market_clean['market_id'] = df_market_clean['market_id'].astype('int64')
        df_market_clean['event_id'] = pd.to_numeric(df_market_clean['event_id'], errors='coerce').astype('Int64')

        # Drop clean fact first (it references the dims) so dims can be replaced
        with engine.begin() as conn:
            try:
                conn.execute(text('drop table if exists polymarket.fact_market_event_clean cascade'))
            except Exception as e:
                logger.warning('Could not drop existing clean fact table: %s', e)

        # Write cleaned dims to DB
        # Build cleaned time dim from available timestamps (events, markets, fact recorded_ts later)
        # Collect candidate timestamps
        time_candidates = []
        if 'end_ts' in df_event_clean.columns:
            time_candidates.append(df_event_clean['end_ts'])
        if 'end_ts' in df_market_clean.columns:
            time_candidates.append(df_market_clean['end_ts'])

        if time_candidates:
            times = pd.concat(time_candidates).dropna().drop_duplicates()
            if len(times):
                times = pd.to_datetime(times, utc=True)
                df_time_clean = pd.DataFrame({'ts': times})
                df_time_clean = df_time_clean.drop_duplicates(subset=['ts'])
                df_time_clean['year'] = df_time_clean['ts'].dt.year
                df_time_clean['month'] = df_time_clean['ts'].dt.month
                df_time_clean['day'] = df_time_clean['ts'].dt.day
                df_time_clean['hour'] = df_time_clean['ts'].dt.hour
        else:
            df_time_clean = pd.DataFrame(columns=['ts','year','month','day','hour'])

        df_event_clean.to_sql('dim_event_clean', engine, schema='polymarket', if_exists='replace', index=False)
        df_market_clean.to_sql('dim_market_clean', engine, schema='polymarket', if_exists='replace', index=False)
        df_tag_clean.to_sql('dim_tag_clean', engine, schema='polymarket', if_exists='replace', index=False)
        df_series_clean.to_sql('dim_series_clean', engine, schema='polymarket', if_exists='replace', index=False)
        # write time dim
        df_time_clean.to_sql('dim_time_clean', engine, schema='polymarket', if_exists='replace', index=False)

        # Build and write event-tag bridge
        if not df_tag_clean.empty:
            df_bridge = df_tag_clean[['event_id', 'tag_id']].dropna().drop_duplicates()
        else:
            df_bridge = pd.DataFrame(columns=['event_id', 'tag_id'])

        if len(df_bridge):
            df_bridge['event_id'] = df_bridge['event_id'].astype('int64')
            df_bridge['tag_id'] = df_bridge['tag_id'].astype('int64')

        df_bridge.to_sql('event_tag_bridge', engine, schema='polymarket', if_exists='replace', index=False)
        with engine.begin() as conn:
            try:
                bridge_count = conn.execute(text('select count(*) from polymarket.event_tag_bridge')).scalar()
            except Exception:
                bridge_count = 0

        logger.info('Wrote cleaned dimensions to NeonDB (dim_*_clean) and event_tag_bridge (rows=%s)', bridge_count)

        # Build fact table by joining cleaned dims (simple join: market -> event -> tag/series by event)
        df_fact = df_market_clean[['market_id','event_id','liquidity','volume','active','closed','end_ts']].copy()
        df_fact = df_fact.rename(columns={'end_ts':'recorded_ts'})

        # map tag_id via event_id when available
        if not df_tag_clean.empty:
            tag_map = df_tag_clean.set_index('event_id')['tag_id'].dropna().to_dict()
            df_fact['tag_id'] = df_fact['event_id'].map(tag_map)
        else:
            df_fact['tag_id'] = None

        # leave series_id null (could be enriched later)
        df_fact['series_id'] = None

        # reorder columns to match clean fact table
        df_fact = df_fact[['market_id','event_id','series_id','tag_id','liquidity','volume','active','closed','recorded_ts']]

        # write to clean fact (this table has FKs to clean dims)
        df_fact.to_sql('fact_market_event_clean', engine, schema='polymarket', if_exists='replace', index=False)

        # final counts
        with engine.begin() as conn:
            cnt = conn.execute(text('select count(*) from polymarket.fact_market_event_clean')).scalar()
        logger.info('Fact table (clean) populated (rows=%s)', cnt)
    except Exception as e:
        logger.warning('Could not execute DDL or populate clean dims/fact: %s', e)


if __name__ == '__main__':
    main()
