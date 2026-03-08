import importlib.util
from pathlib import Path
from sqlalchemy import text, create_engine
import os

# Load db.py dynamically to ensure correct .env path handling
DB_PATH = Path(__file__).resolve().parent / 'db.py'
spec = importlib.util.spec_from_file_location('dbmodule', str(DB_PATH))
dbmodule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(dbmodule)

def list_columns(table_name: str):
    q = text("SELECT column_name FROM information_schema.columns WHERE table_schema='polymarket' AND table_name = :t;")
    with dbmodule.engine.begin() as conn:
        cols = [r[0] for r in conn.execute(q, {"t": table_name}).fetchall()]
    print(f"{table_name}: {cols}")

if __name__ == '__main__':
    for t in ['dim_market', 'dim_event', 'dim_series', 'dim_tag', 'fact_market_event']:
        list_columns(t)
