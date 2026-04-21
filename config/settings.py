from dotenv import load_dotenv
import os

load_dotenv()  # Lee el archivo .env y mete las variables en el entorno del proceso

# Configuración del activo a monitorizar
TICKER = os.getenv("TICKER", "GC=F")  # GC=F es oro (Gold Futures) en Yahoo Finance

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Webhook alternativo (n8n u otro)
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")

# Parámetros de indicadores
SMA_SHORT = int(os.getenv("SMA_SHORT", 50))
SMA_LONG = int(os.getenv("SMA_LONG", 200))
RSI_PERIOD = int(os.getenv("RSI_PERIOD", 14))
HISTORICAL_DAYS = int(os.getenv("HISTORICAL_DAYS", 200))