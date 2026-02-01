# main.py
"""
Bot SPOT 100% automático (EUR + Supabase/PostgreSQL)
- Recibe velas cerradas desde websocket.py (5m)
- 1 posición por símbolo (hasta 5 simultáneas)
- BUY: compra gastando EUR (quoteOrderQty)
- SELL: vende usando balance real del activo base, con stepSize + MIN_NOTIONAL (safe)

⚠️ La estrategia aquí es DEMO (vela verde compra; TP/SL vende).
No es una estrategia rentable por sí sola.
"""

import asyncio
import logging
import os
import signal
from dataclasses import dataclass
from typing import Dict

from dotenv import load_dotenv

from websocket import BinanceWebSocket, KlineEvent
from db import DB
from ordenes import BinanceSpotExecutor

# -----------------------------
# ENV
# -----------------------------
load_dotenv("claves.env")

TIMEFRAME = os.getenv("TIMEFRAME", "5m")
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()]

QUOTE_ASSET = "EUR"
RISK_PCT = float(os.getenv("MAX_RISK_PER_TRADE", "0.02"))
MIN_BUY_EUR = float(os.getenv("MIN_BUY_EUR", "10"))

TP_PCT = float(os.getenv("TP_PCT", "0.01"))
SL_PCT = float(os.getenv("SL_PCT", "0.01"))

if not SYMBOLS:
    raise RuntimeError("No hay SYMBOLS definidos en claves.env")

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("MAIN")


# -----------------------------
# Estado del bot
# -----------------------------
@dataclass
class Position:
    symbol: str
    base_asset: str
    entry_price: float


positions: Dict[str, Position] = {}   # 1 posición por símbolo
last_close_time: Dict[str, int] = {}  # anti-duplicados por vela cerrada

# -----------------------------
# DB + Executor
# -----------------------------
db = DB()
db.init_schema()

executor = BinanceSpotExecutor(db=db)


def base_asset_from_symbol(symbol: str) -> str:
    # Para pares *EUR: base = el prefijo antes de EUR (BTC, ETH, ...)
    if symbol.endswith("EUR"):
        return symbol[:-3]
    return symbol


def load_positions_from_db() -> None:
    rows = db.get_open_positions()
    for r in rows:
        positions[r["symbol"]] = Position(
            symbol=r["symbol"],
            base_asset=r["base_asset"],
            entry_price=float(r["entry_price"]),
        )
    log.info("Posiciones OPEN recuperadas del DB: %s", list(positions.keys()))


def decide_action(event: KlineEvent) -> str:
    """
    Estrategia DEMO:
    - BUY si no hay posición y la vela cierra por encima de la apertura (vela verde).
    - SELL si hay posición y llega a TP o SL según entry_price.
    """
    sym = event.symbol

    if sym not in positions:
        return "BUY" if event.close > event.open else "HOLD"

    pos = positions[sym]
    pnl_pct = (event.close - pos.entry_price) / pos.entry_price

    if pnl_pct >= TP_PCT:
        return "SELL"
    if pnl_pct <= -SL_PCT:
        return "SELL"

    return "HOLD"


def on_kline(event: KlineEvent) -> None:
    # Solo velas cerradas (también se filtra en websocket.py, pero reforzamos)
    if not event.is_closed:
        return

    # Anti-duplicado por símbolo/vela
    prev = last_close_time.get(event.symbol)
    if prev == event.close_time:
        return
    last_close_time[event.symbol] = event.close_time

    log.info(
        "[VELA CERRADA] %s %s | O:%.6f H:%.6f L:%.6f C:%.6f V:%.2f",
        event.symbol, event.interval,
        event.open, event.high, event.low, event.close, event.volume
    )

    action = decide_action(event)
    if action == "HOLD":
        return

    sym = event.symbol
    base_asset = base_asset_from_symbol(sym)

    try:
        if action == "BUY":
            # 1 posición por símbolo
            if sym in positions:
                return

            budget = executor.compute_quote_budget(QUOTE_ASSET, RISK_PCT)
            if budget < MIN_BUY_EUR:
                log.warning("Budget %.2f EUR < MIN_BUY_EUR %.2f. No compro %s.", budget, MIN_BUY_EUR, sym)
                return

            res = executor.place_market_buy_by_quote(sym, budget)
            log.info("BUY OK %s | orderId=%s status=%s", sym, res.order_id, res.status)

            # Guardar posición en DB (para reinicios)
            db.upsert_open_position(
                symbol=sym,
                base_asset=base_asset,
                quote_asset=QUOTE_ASSET,
                entry_price=event.close,
            )
            positions[sym] = Position(symbol=sym, base_asset=base_asset, entry_price=event.close)
            log.info("POS OPEN %s | entry=%.6f", sym, event.close)

        elif action == "SELL":
            # Solo si hay posición abierta
            if sym not in positions:
                return

            # ✅ VENTA ROBUSTA: balance real + stepSize + MIN_NOTIONAL
            free_base = executor.get_free_balance(base_asset)
            if free_base <= 0:
                log.warning("Balance %s=0, no puedo vender %s.", base_asset, sym)
                return

            res = executor.place_market_sell_by_base_safe(sym, free_base)
            log.info("SELL OK %s | orderId=%s status=%s", sym, res.order_id, res.status)

            db.close_position(symbol=sym)
            del positions[sym]
            log.info("POS CLOSED %s", sym)

    except Exception as e:
        log.exception("Fallo ejecutando %s en %s: %s", action, sym, e)


async def run_bot() -> None:
    load_positions_from_db()

    log.info("Iniciando BOT (EUR + PostgreSQL/Supabase)")
    log.info("Símbolos: %s", SYMBOLS)
    log.info("Timeframe: %s", TIMEFRAME)
    log.info("Riesgo: %.2f%% | MIN_BUY_EUR: %.2f | TP: %.2f%% | SL: %.2f%%",
             RISK_PCT * 100, MIN_BUY_EUR, TP_PCT * 100, SL_PCT * 100)

    ws = BinanceWebSocket(symbols=SYMBOLS, interval=TIMEFRAME, on_kline=on_kline)

    loop = asyncio.get_running_loop()

    def shutdown():
        log.warning("Apagando bot...")
        asyncio.create_task(ws.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    await ws.start()


if __name__ == "__main__":
    asyncio.run(run_bot())




