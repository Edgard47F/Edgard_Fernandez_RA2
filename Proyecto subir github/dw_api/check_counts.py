from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path

p = Path(__file__).resolve().parent / '.env'
if p.exists():
    load_dotenv(p)
else:
    load_dotenv()

NEON_CONN = os.getenv('NEON_CONN') or os.getenv('DATABASE_URL')
if not NEON_CONN:
    raise RuntimeError('NEON_CONN or DATABASE_URL not found')

engine = create_engine(NEON_CONN)

tables = ['dim_time_clean','dim_time','dim_event_clean','dim_market_clean','dim_series_clean','dim_tag_clean','fact_market_event_clean','event_tag_bridge']
with engine.begin() as conn:
    for t in tables:
        try:
            cnt = conn.execute(text(f'select count(*) from polymarket."{t}"')).scalar()
        except Exception as e:
            cnt = str(e)
        print(f'{t}: {cnt}')
