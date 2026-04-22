import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from core.state_manager import StateManager
from core.processor import process

from contextlib import asynccontextmanager
from core.extractor import fetch_historical

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Se ejecuta al arrancar la API. Inicializa el histórico si no existe."""
    state = StateManager()
    try:
        if not state.has_historical_data():
            logger.info("Sin histórico detectado. Descargando datos iniciales...")
            df = fetch_historical()
            state.save_records(df)
            logger.info("Histórico inicial cargado correctamente.")
    except Exception as e:
        logger.error("Error en inicialización: %s", e)
    finally:
        state.close()
    yield

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FDPAS API",
    description="Financial Data Pipeline & Alert System — API de indicadores",
    version="1.0.0",
    lifespan=lifespan
)

# CORS necesario para que la landing en GitHub Pages pueda consumir la API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


class IndicatorsResponse(BaseModel):
    """
    Pydantic define el esquema de la respuesta.
    FastAPI lo valida automáticamente y lo incluye en el Swagger.
    """
    ticker: str
    price: float
    sma_short: float | None
    sma_long: float | None
    rsi: float | None
    pct_change: float | None
    records_in_db: int


@app.get("/health")
def health():
    """Endpoint de comprobación básica. Útil para monitorización."""
    return {"status": "ok"}


@app.get("/indicators", response_model=IndicatorsResponse)
def get_indicators():
    """
    Devuelve los indicadores técnicos calculados sobre el histórico local.
    Lee de SQLite, no llama a ninguna API externa.
    """
    state = StateManager()
    try:
        if not state.has_historical_data():
            raise HTTPException(
                status_code=503,
                detail="No hay datos históricos. Ejecuta main.py primero."
            )

        df = state.load_all()
        indicators = process(df)

        from config.settings import TICKER
        return IndicatorsResponse(
            ticker=TICKER,
            records_in_db=len(df),
            **indicators
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error calculando indicadores: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        state.close()