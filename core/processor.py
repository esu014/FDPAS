import logging
import pandas as pd

from config.settings import SMA_SHORT, SMA_LONG, RSI_PERIOD

logger = logging.getLogger(__name__)


def calculate_sma(series: pd.Series, period: int) -> pd.Series:
    """
    Calcula la Media Móvil Simple (SMA) para una serie de precios.
    .rolling(window) agrupa los últimos N valores y .mean() calcula la media.
    """
    return series.rolling(window=period).mean()


def calculate_rsi(series: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """
    Calcula el RSI (Relative Strength Index) de Wilder.
    
    Pasos:
    1. Calcular la diferencia diaria de precios (delta)
    2. Separar subidas (gains) y bajadas (losses)
    3. Calcular la media exponencial de gains y losses
    4. RSI = 100 - (100 / (1 + RS)) donde RS = avg_gain / avg_loss
    """
    delta = series.diff()

    gains = delta.clip(lower=0)   # Solo valores positivos, el resto 0
    losses = -delta.clip(upper=0) # Solo valores negativos invertidos a positivo

    # ewm = Exponential Weighted Mean, alpha=1/period es la fórmula de Wilder
    avg_gain = gains.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = losses.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi


def calculate_pct_change(series: pd.Series) -> pd.Series:
    """
    Calcula la variación porcentual respecto al cierre anterior.
    .pct_change() es un método nativo de pandas que hace exactamente esto.
    """
    return series.pct_change() * 100


def process(df: pd.DataFrame) -> dict:
    """
    Función principal del módulo. Recibe el histórico completo y devuelve
    un diccionario con los valores actuales de cada indicador.
    """
    if df.empty:
        logger.error("El DataFrame recibido está vacío, no se pueden calcular indicadores.")
        raise ValueError("DataFrame vacío en processor.process()")

    close = df["close"] if "close" in df.columns else df["Close"]

    sma_short = calculate_sma(close, SMA_SHORT)
    sma_long = calculate_sma(close, SMA_LONG)
    rsi = calculate_rsi(close)
    pct_change = calculate_pct_change(close)

    # Cogemos el último valor de cada serie (.iloc[-1] = último elemento)
    current_price = close.iloc[-1]
    current_sma_short = sma_short.iloc[-1]
    current_sma_long = sma_long.iloc[-1]
    current_rsi = rsi.iloc[-1]
    current_pct_change = pct_change.iloc[-1]

    result = {
        "price": round(current_price, 4),
        "sma_short": round(current_sma_short, 4) if pd.notna(current_sma_short) else None,
        "sma_long": round(current_sma_long, 4) if pd.notna(current_sma_long) else None,
        "rsi": round(current_rsi, 2) if pd.notna(current_rsi) else None,
        "pct_change": round(current_pct_change, 4) if pd.notna(current_pct_change) else None,
    }

    logger.info(
        "Indicadores calculados — Precio: %.4f | SMA%d: %s | SMA%d: %s | RSI: %s | Variación: %s%%",
        result["price"],
        SMA_SHORT, result["sma_short"],
        SMA_LONG, result["sma_long"],
        result["rsi"],
        result["pct_change"]
    )

    return result