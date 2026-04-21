import time
import logging
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone
import zoneinfo

from config.settings import TICKER, HISTORICAL_DAYS

logger = logging.getLogger(__name__)

# El mercado de futuros del oro opera en la zona horaria de Chicago
MARKET_TZ = zoneinfo.ZoneInfo("America/Chicago")


def is_market_open() -> bool:
    """
    Comprueba si el mercado de futuros está operativo.
    Opera de domingo 17:00 a viernes 16:00 hora Chicago.
    """
    now = datetime.now(tz=MARKET_TZ)
    weekday = now.weekday()  # 0=lunes, 6=domingo

    # Sábado completo cerrado
    if weekday == 5:
        logger.warning("Mercado cerrado: sábado.")
        return False

    # Domingo antes de las 17:00 cerrado
    if weekday == 6 and now.hour < 17:
        logger.warning("Mercado cerrado: domingo antes de las 17:00 Chicago.")
        return False

    # Viernes después de las 16:00 cerrado
    if weekday == 4 and now.hour >= 16:
        logger.warning("Mercado cerrado: viernes después de las 16:00 Chicago.")
        return False

    logger.info("Mercado abierto. Hora Chicago: %s", now.strftime("%A %H:%M"))
    return True


def _download_with_retry(ticker: str, period: str, max_retries: int = 3) -> pd.DataFrame:
    """
    Descarga datos de yfinance con lógica de reintento exponencial.
    Si falla 3 veces lanza la excepción para que el orquestador la gestione.
    """
    delay = 1  # segundos iniciales de espera

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("Descargando datos para %s (intento %d/%d)...", ticker, attempt, max_retries)
            df = yf.download(ticker, period=period, auto_adjust=True, progress=False)

            if df.empty:
                raise ValueError(f"yfinance devolvió un DataFrame vacío para {ticker}")

            logger.info("Descarga completada: %d registros.", len(df))
            return df

        except Exception as e:
            logger.warning("Intento %d fallido: %s", attempt, e)
            if attempt < max_retries:
                logger.info("Reintentando en %d segundos...", delay)
                time.sleep(delay)
                delay *= 2  # exponential backoff
            else:
                logger.error("Todos los reintentos agotados para %s.", ticker)
                raise


def fetch_historical(ticker: str = TICKER) -> pd.DataFrame:
    """
    Descarga el histórico completo (HISTORICAL_DAYS días).
    Se llama solo en la primera ejecución.
    """
    period = f"{HISTORICAL_DAYS}d"
    logger.info("Descargando histórico de %s días para %s...", HISTORICAL_DAYS, ticker)
    return _download_with_retry(ticker, period=period)


def fetch_latest(ticker: str = TICKER) -> pd.DataFrame:
    """
    Descarga solo los últimos 5 días para obtener el precio más reciente.
    Se llama en cada ejecución posterior a la primera.
    """
    logger.info("Descargando precio reciente para %s...", ticker)
    return _download_with_retry(ticker, period="5d")