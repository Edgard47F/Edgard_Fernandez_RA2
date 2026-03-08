# db.py
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Carga .env desde la misma carpeta que este archivo (ruta absoluta)
BASE_DIR = Path(__file__).resolve().parent
DOTENV_PATH = BASE_DIR / ".env"
load_dotenv(DOTENV_PATH, override=False)

def get_database_url() -> str:
    url = os.getenv("NEON_CONN") or os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError(f"No existe NEON_CONN ni DATABASE_URL. Revisa {DOTENV_PATH} (NEON_CONN=... or DATABASE_URL=...)")
    return url

def get_engine() -> Engine:
    return create_engine(get_database_url(), pool_pre_ping=True)

engine = get_engine()
