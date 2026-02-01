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
from typing import Dict, Optional

from dotenv import load_dotenv

from websocket import BinanceWebSocket, KlineEvent
from db import DB
from ordenes import BinanceSpotExecutor
from strategy_engine import StrategyEngine, StrategyResult
from taapi_client import TAAPIClient

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
MAX_CANDLES = int(os.getenv("MAX_CANDLES", "200"))
EXECUTE_ORDERS = os.getenv("EXECUTE_ORDERS", "false").strip().lower() == "true"
TAAPI_ENABLED = os.getenv("TAAPI_ENABLED", "true").strip().lower() == "true"
STRATEGY_MIN_ACTIVE = int(os.getenv("STRATEGY_MIN_ACTIVE", "1"))
STRATEGY_MIN_SCORE = float(os.getenv("STRATEGY_MIN_SCORE", "0"))

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
strategy_engine = StrategyEngine(
    entry_dir="strategy_entry",
    exit_dir="strategy_exit",
    max_candles=MAX_CANDLES,
)
taapi_client: Optional[TAAPIClient]
if TAAPI_ENABLED:
    try:
        taapi_client = TAAPIClient(db=db)
    except RuntimeError as exc:
        log.warning("TAAPI desactivado: %s", exc)
        taapi_client = None
else:
    taapi_client = None


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


def open_position(symbol: str, entry_price: float) -> None:
    base_asset = base_asset_from_symbol(symbol)
    positions[symbol] = Position(
        symbol=symbol,
        base_asset=base_asset,
        entry_price=entry_price,
    )
    db.upsert_open_position(
        symbol=symbol,
        base_asset=base_asset,
        quote_asset=QUOTE_ASSET,
        entry_price=entry_price,
    )


def close_position(symbol: str) -> None:
    if symbol in positions:
        positions.pop(symbol, None)
    db.close_position(symbol=symbol)

    
def _log_strategy_hits(symbol: str, results: list[StrategyResult], kind: str) -> None:
    activas = [res for res in results if res.active]
    if not activas:
        return
    for res in activas:
        log.info("[%s] %s | %s (score=%.2f) -> %s", kind, symbol, res.name, res.score, res.message)


def _persist_strategy_signals(
    *,
    symbol: str,
    interval: str,
    close_time: int,
    results: list[StrategyResult],
) -> None:
    for res in results:
        try:
            db.log_strategy_signal(
                symbol=symbol,
                interval=interval,
                close_time=close_time,
                strategy_name=res.name,
                strategy_type=res.tipo,
                active=res.active,
                score=res.score,
                message=res.message,
            )
        except Exception as exc:
            log.warning("No se pudo registrar señal %s: %s", res.name, exc)

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

    df = strategy_engine.update_candle(
        symbol=event.symbol,
        candle={
            "open_time": event.open_time,
            "close_time": event.close_time,
            "open": event.open,
            "high": event.high,
            "low": event.low,
            "close": event.close,
            "volume": event.volume,
        },
    )

    indicators = None
    if taapi_client is not None:
        try:
            snapshot = taapi_client.fetch_snapshot(
                symbol=event.symbol,
                interval=event.interval,
                close_time=event.close_time,
            )
            indicators = snapshot.get("indicators")
        except Exception as exc:
            log.warning("TAAPI error para %s: %s", event.symbol, exc)

    if event.symbol not in positions:
        resultados = strategy_engine.evaluate_entries(df=df, indicators=indicators)
        _persist_strategy_signals(
            symbol=event.symbol,
            interval=event.interval,
            close_time=event.close_time,
            results=resultados,
        )
        _log_strategy_hits(event.symbol, resultados, "ENTRADA")
        seleccionadas = StrategyEngine.select_signals(
            resultados,
            min_active=STRATEGY_MIN_ACTIVE,
            min_score=STRATEGY_MIN_SCORE,
        )
        if seleccionadas:
            if EXECUTE_ORDERS:
                budget = max(MIN_BUY_EUR, executor.compute_quote_budget(QUOTE_ASSET, RISK_PCT))
                if budget <= 0:
                    log.warning("Sin balance disponible para %s", QUOTE_ASSET)
                    return
                try:
                    res = executor.place_market_buy_by_quote(event.symbol, budget)
                    log.info("BUY ejecutado: %s %s", res.symbol, res.client_order_id)
                    open_position(event.symbol, entry_price=event.close)
                except Exception as exc:
                    log.error("Error en BUY %s: %s", event.symbol, exc)
            else:
                log.info("Señal de entrada detectada (EXECUTE_ORDERS=false).")
        return

    resultados = strategy_engine.evaluate_exits(df=df, indicators=indicators)
    _persist_strategy_signals(
        symbol=event.symbol,
        interval=event.interval,
        close_time=event.close_time,
        results=resultados,
    )
    _log_strategy_hits(event.symbol, resultados, "SALIDA")
    seleccionadas = StrategyEngine.select_signals(
        resultados,
        min_active=STRATEGY_MIN_ACTIVE,
        min_score=STRATEGY_MIN_SCORE,
    )
    if seleccionadas:
        if EXECUTE_ORDERS:
            base_asset = positions[event.symbol].base_asset
            base_qty = executor.get_free_balance(base_asset)
            if base_qty <= 0:
                log.warning("Sin balance de %s para vender", base_asset)
                return
            try:
                res = executor.place_market_sell_by_base_safe(event.symbol, base_qty)
                log.info("SELL ejecutado: %s %s", res.symbol, res.client_order_id)
                close_position(event.symbol)
            except Exception as exc:
                log.error("Error en SELL %s: %s", event.symbol, exc)
        else:
            log.info("Señal de salida detectada (EXECUTE_ORDERS=false).")


async def run_bot() -> None:
    load_positions_from_db()

    log.info("Iniciando BOT (EUR + PostgreSQL/Supabase)")
    log.info("Símbolos: %s", SYMBOLS)
    log.info("Timeframe: %s", TIMEFRAME)
    log.info("Modo órdenes: %s", "ACTIVO" if EXECUTE_ORDERS else "SOLO DATOS")
    log.info("Riesgo: %.2f%% | MIN_BUY_EUR: %.2f | TP: %.2f%% | SL: %.2f%%",
             RISK_PCT * 100, MIN_BUY_EUR, TP_PCT * 100, SL_PCT * 100)
    log.info(
        "Selector estrategias: min_activas=%d | score_min=%.2f",
        STRATEGY_MIN_ACTIVE,
        STRATEGY_MIN_SCORE,
    )

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




