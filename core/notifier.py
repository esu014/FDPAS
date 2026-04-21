import asyncio
import logging
import aiohttp

from config.settings import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, WEBHOOK_URL, SMA_SHORT

logger = logging.getLogger(__name__)


# ─── Reglas de alerta ────────────────────────────────────────────────────────

def _check_rules(indicators: dict) -> list[str]:
    """
    Evalúa las reglas sobre los indicadores y devuelve una lista de alertas.
    Si la lista está vacía, no hay nada que notificar.
    """
    alerts = []
    price = indicators.get("price")
    sma_short = indicators.get("sma_short")
    rsi = indicators.get("rsi")
    pct_change = indicators.get("pct_change")

    # Regla 1: precio cruza por debajo de la SMA50
    if price and sma_short and price < sma_short:
        alerts.append(
            f"⚠️ Precio ({price}) por debajo de SMA{SMA_SHORT} ({sma_short}). Señal bajista."
        )

    # Regla 2: RSI sobrecompra
    if rsi and rsi > 70:
        alerts.append(f"🔴 RSI en sobrecompra: {rsi:.2f} (>70).")

    # Regla 3: RSI sobreventa
    if rsi and rsi < 30:
        alerts.append(f"🟢 RSI en sobreventa: {rsi:.2f} (<30).")

    # Regla 4: caída mayor del 3% en una sesión
    if pct_change and pct_change < -3.0:
        alerts.append(f"🚨 Caída del {pct_change:.2f}% en la sesión.")

    return alerts


# ─── Clientes de envío ───────────────────────────────────────────────────────

async def _send_telegram(message: str, retries: int = 3):
    """Envía un mensaje a Telegram con reintento exponencial."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram no configurado, saltando notificación.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    delay = 1

    async with aiohttp.ClientSession() as session:
        for attempt in range(1, retries + 1):
            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        logger.info("Alerta enviada a Telegram correctamente.")
                        return
                    else:
                        logger.warning("Telegram respondió %d en intento %d.", resp.status, attempt)
            except Exception as e:
                logger.warning("Error enviando a Telegram (intento %d): %s", attempt, e)

            if attempt < retries:
                await asyncio.sleep(delay)
                delay *= 2

    logger.error("No se pudo enviar la alerta a Telegram tras %d intentos.", retries)


async def _send_webhook(payload: dict, retries: int = 3):
    """Envía un payload JSON a un webhook (n8n u otro) con reintento exponencial."""
    if not WEBHOOK_URL:
        logger.warning("WEBHOOK_URL no configurado, saltando notificación.")
        return

    delay = 1

    async with aiohttp.ClientSession() as session:
        for attempt in range(1, retries + 1):
            try:
                async with session.post(WEBHOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status in (200, 201):
                        logger.info("Payload enviado al webhook correctamente.")
                        return
                    else:
                        logger.warning("Webhook respondió %d en intento %d.", resp.status, attempt)
            except Exception as e:
                logger.warning("Error enviando al webhook (intento %d): %s", attempt, e)

            if attempt < retries:
                await asyncio.sleep(delay)
                delay *= 2

    logger.error("No se pudo enviar al webhook tras %d intentos.", retries)


# ─── Punto de entrada público ────────────────────────────────────────────────

async def evaluate_and_notify(indicators: dict):
    """
    Función pública del módulo. Evalúa las reglas y dispara
    las notificaciones necesarias de forma asíncrona.
    """
    alerts = _check_rules(indicators)

    if not alerts:
        logger.info("Sin alertas en este ciclo.")
        return

    message = "\n".join(alerts)
    logger.warning("Alertas detectadas:\n%s", message)

    # Lanza ambos envíos en paralelo si están configurados
    await asyncio.gather(
        _send_telegram(message),
        _send_webhook({"alerts": alerts, "indicators": indicators})
    )