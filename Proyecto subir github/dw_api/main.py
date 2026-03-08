# main.py  (API FastAPI leyendo de NeonDB y cargando variables desde .env)

import os
import logging
from typing import Optional, List, Any, Dict

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---- Cargar .env de forma robusta (ruta absoluta) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOTENV_PATH = os.path.join(BASE_DIR, ".env")

loaded = load_dotenv(DOTENV_PATH, override=False)

NEON_CONN = os.getenv("NEON_CONN") or os.getenv("DATABASE_URL")

# Si dotenv no carga la variable por alguna razón, intentar parsear .env manualmente
if not NEON_CONN:
    try:
        with open(DOTENV_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                key = key.strip()
                val = val.strip()
                if key in ("NEON_CONN", "DATABASE_URL"):
                    NEON_CONN = val.strip().strip('"').strip("'")
                    break
    except FileNotFoundError:
        pass

if isinstance(NEON_CONN, str):
    NEON_CONN = NEON_CONN.strip().strip('"').strip("'")
if not NEON_CONN:
    file_exists = os.path.exists(DOTENV_PATH)
    raise RuntimeError(
        f"No existe NEON_CONN ni DATABASE_URL. Revisa {DOTENV_PATH} (debe tener una línea: NEON_CONN=... o DATABASE_URL=...) "
        f"| dotenv_loaded={loaded} | dotenv_exists={file_exists}"
    )

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Log the loaded connection string (masked)
masked = None
if NEON_CONN:
    if len(NEON_CONN) > 40:
        masked = NEON_CONN[:20] + "..." + NEON_CONN[-10:]
    else:
        masked = NEON_CONN
    logger.info("Using NEON_CONN: %s", masked)

try:
    engine = create_engine(NEON_CONN, pool_pre_ping=True)
except Exception as e:
    logger.exception("Failed to create engine with provided NEON_CONN")
    raise

app = FastAPI(
    title="DW API (NeonDB)",
    version="1.0.0",
    description="API de solo lectura para consultar el Data Warehouse almacenado en NeonDB.",
)

class Event(BaseModel):
    event_id: int
    title: Optional[str] = None
    category: Optional[str] = None
    end_ts: Optional[str] = None

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

def rows_to_dicts(result) -> List[Dict[str, Any]]:
    cols = list(result.keys())
    return [dict(zip(cols, row)) for row in result.fetchall()]


# --- Fact table model and DDL ---
class Fact(BaseModel):
    fact_id: int
    market_id: Optional[int] = None
    event_id: Optional[int] = None
    series_id: Optional[int] = None
    tag_id: Optional[int] = None
    liquidity: Optional[float] = None
    volume: Optional[float] = None
    active: Optional[bool] = None
    closed: Optional[bool] = None
    recorded_ts: Optional[str] = None


FACT_TABLE_DDL = """
create table if not exists polymarket.fact_market_event (
    fact_id bigserial primary key,
    market_id bigint,
    event_id bigint,
    series_id bigint,
    tag_id bigint,
    liquidity double precision,
    volume double precision,
    active boolean,
    closed boolean,
    recorded_ts timestamptz,
    foreign key (market_id) references polymarket.dim_market(market_id),
    foreign key (event_id) references polymarket.dim_event(event_id)
);
"""

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
            {"limit": limit, "offset": offset, "active": active, "closed": closed, "event_id": event_id, "q": q},
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


@app.get("/facts", response_model=list[Fact], tags=["facts"])
def list_facts(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    market_id: Optional[int] = Query(None),
    event_id: Optional[int] = Query(None),
):
    sql = """
    select fact_id, market_id, event_id, series_id, tag_id,
           liquidity, volume, active, closed, recorded_ts
    from polymarket.fact_market_event
    where (:market_id is null or market_id = :market_id)
      and (:event_id is null or event_id = :event_id)
    order by fact_id desc
    limit :limit offset :offset
    """
    with engine.begin() as conn:
        res = conn.execute(
            text(sql),
            {"limit": limit, "offset": offset, "market_id": market_id, "event_id": event_id},
        )
        return rows_to_dicts(res)


@app.get("/facts/{fact_id}", response_model=Fact, tags=["facts"])
def get_fact(fact_id: int):
    sql = """
    select fact_id, market_id, event_id, series_id, tag_id,
           liquidity, volume, active, closed, recorded_ts
    from polymarket.fact_market_event
    where fact_id = :fact_id
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"fact_id": fact_id})
        row = res.mappings().first()
    if not row:
        raise HTTPException(status_code=404, detail="Fact not found")
    return dict(row)


@app.post("/admin/create_fact_table", tags=["admin"])
def create_fact_table():
    with engine.begin() as conn:
        conn.execute(text(FACT_TABLE_DDL))
    return {"created": True}


# --- Additional project endpoints required by RA2 spec ---
@app.get("/markets/top-volume", tags=["markets"])
def markets_top_volume(category: Optional[str] = Query(None), limit: int = Query(10, ge=1, le=100)):
    sql = """
    select market_id, question, category, volume
    from polymarket.dim_market
    where (:category is null or category = :category)
    order by volume desc nulls last
    limit :limit
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"category": category, "limit": limit})
        return rows_to_dicts(res)


@app.get("/series/{series_id}/probability", tags=["series"])
def series_probability(series_id: int, limit: int = Query(200, ge=1, le=10000)):
    # Attempts to return an evolution of average probability for a series.
    sql = """
    select recorded_ts, avg(probability) as avg_probability
    from polymarket.dim_series
    where series_id = :series_id
    group by recorded_ts
    order by recorded_ts asc
    limit :limit
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"series_id": series_id, "limit": limit})
        return rows_to_dicts(res)


@app.get("/tags/search", tags=["tags"])
def tags_search(name: str = Query(..., min_length=1)):
    sql = """
    select t.tag_id, t.name as tag_name, e.event_id, e.title as event_title
    from polymarket.dim_tag t
    left join polymarket.dim_event e on e.event_id = t.event_id
    where t.name ilike '%' || :name || '%'
    order by t.name
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"name": name})
        return rows_to_dicts(res)


@app.get("/events/closing-soon", tags=["events"])
def events_closing_soon(hours: int = Query(48, ge=1, le=168)):
    sql = """
    select event_id, title, category, end_ts
    from polymarket.dim_event
    where end_ts >= now() and end_ts <= now() + (:hours || ' hours')::interval
    order by end_ts asc
    """
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"hours": hours})
        return rows_to_dicts(res)
