# main.py  (API FastAPI leyendo de NeonDB y cargando variables desde fastapi.env)

import os
from typing import Optional, List, Any, Dict

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1) Cargar variables de entorno desde fastapi.env
load_dotenv("fastapi.env", override=False)

NEON_CONN = os.getenv("NEON_CONN")
if not NEON_CONN:
    raise RuntimeError(
        "No existe NEON_CONN. Revisa fastapi.env (debe tener una línea: NEON_CONN=...)"
    )

# 2) Crear engine a NeonDB
engine = create_engine(NEON_CONN, pool_pre_ping=True)

# 3) App + metadata (sale en /docs y /redoc)
app = FastAPI(
    title="DW API (NeonDB)",
    version="1.0.0",
    description="API de solo lectura para consultar el Data Warehouse almacenado en NeonDB.",
)

# ---------
# Modelos de respuesta (para documentar)
# ---------
class Event(BaseModel):
    event_id: int
    title: Optional[str] = None
    category: Optional[str] = None
    end_ts: Optional[str] = None  # ISO string / str

class Market(BaseModel):
    market_id: int
    question: Optional[str] = None
    active: Optional[bool] = None
    closed: Optional[bool] = None
    archived: Optional[bool] = None
    category: Optional[str] = None
    liquidity: Optional[float] = None
    volume: Optional[float] = None
    event_id: Optional[int] = None
    end_ts: Optional[str] = None

# ---------
# Util: rows -> dict
# ---------
def rows_to_dicts(result) -> List[Dict[str, Any]]:
    cols = list(result.keys())
    return [dict(zip(cols, row)) for row in result.fetchall()]

# ---------
# Endpoints
# ---------
@app.get("/health", tags=["system"])
def health():
    with engine.begin() as conn:
        ok = conn.execute(text("select 1")).scalar()
    return {"ok": True, "db": ok}

@app.get("/events", response_model=list[Event], tags=["events"])
def list_events(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    q: Optional[str] = Query(None, description="Filtro por texto en title (ILIKE)."),
):
    sql = """
    select event_id, title, category, end_ts
    from polymarket.dim_event
    where (:q is null or title ilike '%' || :q || '%')
    order by event_id desc
    limit :limit offset :offset
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"limit": limit, "offset": offset, "q": q})
        return rows_to_dicts(res)

@app.get("/events/{event_id}", response_model=Event, tags=["events"])
def get_event(event_id: int):
    sql = """
    select event_id, title, category, end_ts
    from polymarket.dim_event
    where event_id = :event_id
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"event_id": event_id})
        row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Event not found")
    return dict(row)

@app.get("/markets", response_model=list[Market], tags=["markets"])
def list_markets(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    active: Optional[bool] = Query(None),
    closed: Optional[bool] = Query(None),
    event_id: Optional[int] = Query(None),
    q: Optional[str] = Query(None, description="Filtro por texto en question (ILIKE)."),
):
    sql = """
    select market_id, question, active, closed, archived, category,
           liquidity, volume, event_id, end_ts
    from polymarket.dim_market
    where
      (:active is null or active = :active) and
      (:closed is null or closed = :closed) and
      (:event_id is null or event_id = :event_id) and
      (:q is null or question ilike '%' || :q || '%')
    order by market_id desc
    limit :limit offset :offset
    """
    with engine.begin() as conn:
        res = conn.execute(
            text(sql),
            {
                "limit": limit,
                "offset": offset,
                "active": active,
                "closed": closed,
                "event_id": event_id,
                "q": q,
            },
        )
        return rows_to_dicts(res)

@app.get("/markets/{market_id}", response_model=Market, tags=["markets"])
def get_market(market_id: int):
    sql = """
    select market_id, question, active, closed, archived, category,
           liquidity, volume, event_id, end_ts
    from polymarket.dim_market
    where market_id = :market_id
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"market_id": market_id})
        row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Market not found")
    return dict(row)
