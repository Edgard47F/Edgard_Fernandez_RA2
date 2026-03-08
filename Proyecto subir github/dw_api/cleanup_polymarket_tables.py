"""Cleanup helper: drop non-clean and empty tables in schema polymarket.

Usage:
  python cleanup_polymarket_tables.py

This script uses the same .env discovery as the loader and will drop tables
matching these rules:
 - Any table that DOES NOT end with '_clean' will be dropped.
 - Any table (clean or not) that has zero rows will be dropped.

Be careful: drops are performed with CASCADE.
"""
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DOTENV_CANDIDATES = [BASE_DIR / '.env', BASE_DIR.parent / '.env']
for p in DOTENV_CANDIDATES:
    if p.exists():
        load_dotenv(p)
        break

NEON_CONN = os.getenv('NEON_CONN') or os.getenv('DATABASE_URL')
if NEON_CONN:
    NEON_CONN = NEON_CONN.strip().strip('"').strip("'")

if not NEON_CONN:
    raise RuntimeError('NEON_CONN or DATABASE_URL not found in .env')

engine = create_engine(NEON_CONN)


def list_tables():
    q = """
    select table_name
    from information_schema.tables
    where table_schema = 'polymarket' and table_type = 'BASE TABLE'
    order by table_name
    """
    with engine.begin() as conn:
        rows = conn.execute(text(q)).fetchall()
    return [r[0] for r in rows]


def row_count(table):
    with engine.begin() as conn:
        try:
            return conn.execute(text(f'select count(*) from polymarket."{table}"')).scalar()
        except Exception:
            return None


def drop_table(table):
    with engine.begin() as conn:
        conn.execute(text(f'drop table if exists polymarket."{table}" cascade'))


def main():
    tables = list_tables()
    logger.info('Found tables: %s', tables)

    to_drop = []
    details = []
    for t in tables:
        cnt = row_count(t)
        details.append((t, cnt))
        # Drop if not a _clean table, or if empty (zero rows)
        if (not t.endswith('_clean')) or (isinstance(cnt, int) and cnt == 0):
            to_drop.append((t, cnt))

    if not to_drop:
        logger.info('No tables to drop.')
        return

    logger.info('Tables slated for drop: %s', to_drop)

    # proceed to drop
    dropped = []
    for t, cnt in to_drop:
        try:
            drop_table(t)
            dropped.append((t, cnt))
            logger.info('Dropped table %s (rows=%s)', t, cnt)
        except Exception as e:
            logger.warning('Failed to drop %s: %s', t, e)

    remaining = list_tables()
    logger.info('Dropped %s tables. Remaining tables: %s', len(dropped), remaining)


if __name__ == '__main__':
    main()
