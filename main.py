import asyncio
import logging
import sys
from pathlib import Path

from config.settings import TICKER
from core.extractor import is_market_open, fetch_historical, fetch_latest
from core.state_manager import StateManager
from core.processor import process
from core.notifier import evaluate_and_notify


def setup_logging():
    """
    Configura el sistema de logging centralizado.
    Escribe simultáneamente en archivo y en consola.
    """
    Path("logs").mkdir(exist_ok=True)

    log_format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Handler 1: escribe en archivo
            logging.FileHandler("logs/pipeline.log", encoding="utf-8"),
            # Handler 2: escribe en consola
            logging.StreamHandler(sys.stdout),
        ]
    )


logger = logging.getLogger(__name__)


async def run():
    """
    Flujo principal del pipeline:
    1. Comprobar si el mercado está abierto
    2. Cargar o descargar histórico
    3. Calcular indicadores
    4. Evaluar reglas y notificar si procede
    """
    logger.info("=" * 60)
    logger.info("Iniciando FDPAS para ticker: %s", TICKER)
    logger.info("=" * 60)

    # Paso 1: comprobar horario de mercado
    if not is_market_open():
        logger.info("Mercado cerrado. Abortando ejecución.")
        return

    state = StateManager()

    try:
        # Paso 2a: primera ejecución, descargar histórico completo
        if not state.has_historical_data():
            logger.info("Primera ejecución detectada. Descargando histórico completo...")
            df = fetch_historical()
            state.save_records(df)
        else:
            # Paso 2b: ejecuciones posteriores, solo precio reciente
            logger.info("Histórico existente. Descargando precio reciente...")
            df_latest = fetch_latest()
            state.save_records(df_latest)

        # Paso 3: cargar todo el histórico de la BD y calcular indicadores
        df_all = state.load_all()
        indicators = process(df_all)

        # Paso 4: evaluar reglas y notificar
        await evaluate_and_notify(indicators)

    except Exception as e:
        logger.error("Error crítico en el pipeline: %s", e, exc_info=True)
        raise

    finally:
        state.close()
        logger.info("Pipeline finalizado.")


if __name__ == "__main__":
    setup_logging()
    asyncio.run(run())