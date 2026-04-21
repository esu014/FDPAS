import sqlite3
import logging
import pandas as pd
from pathlib import Path

logger = logging.getLogger(__name__)

DB_PATH = Path("logs/market_data.db")


class StateManager:
    def __init__(self):
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(DB_PATH)
        self._create_table()
        logger.info("StateManager inicializado. Base de datos: %s", DB_PATH)

    def _create_table(self):
        """Crea la tabla si no existe. El guión bajo indica método privado por convención."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS prices (
                date TEXT PRIMARY KEY,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL
            )
        """)
        self.conn.commit()

    def has_historical_data(self) -> bool:
        """Devuelve True si ya hay datos en la base de datos."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM prices")
        count = cursor.fetchone()[0]
        logger.info("Registros en base de datos: %d", count)
        return count > 0
    
    def save_records(self, df: pd.DataFrame):
        """
        Recibe un DataFrame de pandas con columnas OHLCV y lo inserta en la BD.
        INSERT OR IGNORE evita duplicados si ya existe esa fecha.
        """
        # yfinance moderno devuelve MultiIndex en columnas, lo aplanamos
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        records = df[["Open", "High", "Low", "Close", "Volume"]].copy()
        records.index = records.index.astype(str)

        inserted = 0
        for date, row in records.iterrows():
            self.conn.execute("""
                INSERT OR IGNORE INTO prices (date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (date, float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"]), float(row["Volume"])))
            inserted += 1

        self.conn.commit()
        logger.info("Registros insertados/ignorados: %d", inserted)

    def load_all(self) -> pd.DataFrame:
        """Carga todo el histórico de la BD en un DataFrame ordenado por fecha."""
        df = pd.read_sql("SELECT * FROM prices ORDER BY date ASC", self.conn)
        df["date"] = pd.to_datetime(df["date"])
        df.set_index("date", inplace=True)
        logger.info("Registros cargados de la BD: %d", len(df))
        return df

    def close(self):
        self.conn.close()
        logger.info("Conexión a la base de datos cerrada.")