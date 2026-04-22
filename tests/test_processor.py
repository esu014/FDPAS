import pytest
import pandas as pd
import numpy as np
from core.processor import calculate_sma, calculate_rsi, calculate_pct_change, process


# ─── Fixtures ────────────────────────────────────────────────────────────────
# Un fixture en pytest es un bloque de datos reutilizable entre tests.
# @pytest.fixture define que esta función es un fixture, no un test.

@pytest.fixture
def flat_series():
    """Serie con precio constante de 100. Útil para verificar casos límite."""
    return pd.Series([100.0] * 60)


@pytest.fixture
def rising_series():
    """Serie con precios crecientes del 1 al 60."""
    return pd.Series(range(1, 61), dtype=float)


@pytest.fixture
def sample_dataframe():
    """DataFrame mínimo con columna 'close' para testear process()."""
    dates = pd.date_range(start="2024-01-01", periods=60, freq="D")
    closes = pd.Series(range(100, 160), dtype=float).values
    return pd.DataFrame({"close": closes}, index=dates)


# ─── Tests de calculate_sma ───────────────────────────────────────────────────

def test_sma_length_matches_input(rising_series):
    """La SMA debe devolver una Serie de la misma longitud que la entrada."""
    result = calculate_sma(rising_series, period=10)
    assert len(result) == len(rising_series)


def test_sma_first_values_are_nan(rising_series):
    """Los primeros N-1 valores deben ser NaN porque no hay suficientes datos."""
    result = calculate_sma(rising_series, period=10)
    assert result.iloc[:9].isna().all()


def test_sma_flat_series_equals_value(flat_series):
    """Con precios constantes la SMA debe ser igual al precio."""
    result = calculate_sma(flat_series, period=10)
    # Ignoramos los primeros NaN y comprobamos el resto
    valid = result.dropna()
    assert (valid == 100.0).all()


def test_sma_known_value(rising_series):
    """Verificamos un valor concreto: SMA10 en posición 9 debe ser 5.5 (media de 1..10)."""
    result = calculate_sma(rising_series, period=10)
    assert result.iloc[9] == pytest.approx(5.5)


# ─── Tests de calculate_rsi ───────────────────────────────────────────────────

def test_rsi_range(rising_series):
    """El RSI siempre debe estar entre 0 y 100."""
    result = calculate_rsi(rising_series, period=14)
    valid = result.dropna()
    assert (valid >= 0).all() and (valid <= 100).all()


def test_rsi_flat_series(flat_series):
    """Con precios constantes no hay subidas ni bajadas, el RSI debe ser NaN o neutro."""
    result = calculate_rsi(flat_series, period=14)
    # Con deltas todos a 0, avg_loss es 0 y hay división por cero -> NaN o 100
    valid = result.dropna()
    # Aceptamos que sean NaN o 100 (ambos son comportamientos válidos)
    assert valid.isna().all() or (valid == 100.0).all()


def test_rsi_rising_market_above_50(rising_series):
    """En un mercado consistentemente alcista el RSI debe estar por encima de 50."""
    result = calculate_rsi(rising_series, period=14)
    valid = result.dropna()
    assert (valid > 50).all()


# ─── Tests de calculate_pct_change ───────────────────────────────────────────

def test_pct_change_first_value_is_nan():
    """El primer valor siempre es NaN porque no hay precio anterior."""
    series = pd.Series([100.0, 110.0, 121.0])
    result = calculate_pct_change(series)
    assert pd.isna(result.iloc[0])


def test_pct_change_known_value():
    """De 100 a 110 la variación debe ser exactamente 10%."""
    series = pd.Series([100.0, 110.0])
    result = calculate_pct_change(series)
    assert result.iloc[1] == pytest.approx(10.0)


def test_pct_change_negative():
    """De 100 a 90 la variación debe ser exactamente -10%."""
    series = pd.Series([100.0, 90.0])
    result = calculate_pct_change(series)
    assert result.iloc[1] == pytest.approx(-10.0)


# ─── Tests de process() ──────────────────────────────────────────────────────

def test_process_returns_dict(sample_dataframe):
    """process() debe devolver un diccionario."""
    result = process(sample_dataframe)
    assert isinstance(result, dict)


def test_process_has_required_keys(sample_dataframe):
    """El diccionario debe contener todas las claves esperadas."""
    result = process(sample_dataframe)
    assert "price" in result
    assert "sma_short" in result
    assert "sma_long" in result
    assert "rsi" in result
    assert "pct_change" in result


def test_process_empty_dataframe_raises():
    """Con un DataFrame vacío process() debe lanzar ValueError."""
    with pytest.raises(ValueError):
        process(pd.DataFrame())


def test_process_price_is_last_close(sample_dataframe):
    """El precio debe ser el último valor de cierre del DataFrame."""
    result = process(sample_dataframe)
    assert result["price"] == pytest.approx(159.0)