"""
websocket.py
------------------------------------
Binance Spot WebSocket feeder
- Velas (klines) en tiempo real
- Timeframe configurable por ENV
- Hasta 5 símbolos
- Reintento automático
- Pensado para bots 100% automáticos

Requisitos:
    pip install websockets python-dotenv

Ejecución:
    python websocket.py
"""

import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass
from typing import List, Optional, Callable

import websockets
from websockets.exceptions import ConnectionClosed
from dotenv import load_dotenv

# ===============================
# Cargar variables de entorno
# ===============================
load_dotenv("claves.env")

TIMEFRAME = os.getenv("TIMEFRAME", "5m")
SYMBOLS = os.getenv("SYMBOLS", "").split(",")

if not SYMBOLS or SYMBOLS == [""]:
    raise RuntimeError("No hay SYMBOLS definidos en claves.env")

# ===============================
# Constantes Binance
# ===============================
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="

# ===============================
# Modelo de evento
# ===============================
@dataclass(frozen=True)
class KlineEvent:
    symbol: str
    interval: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    is_closed: bool

# ===============================
# WebSocket principal
# ===============================
class BinanceWebSocket:
    def __init__(
        self,
        symbols: List[str],
        interval: str,
        on_kline: Optional[Callable[[KlineEvent], None]] = None,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.interval = interval
        self.on_kline = on_kline
        self._stop = False
        self.ws = None

        self.log = logging.getLogger("WebSocket")

    def _build_url(self) -> str:
        streams = [
            f"{symbol.lower()}@kline_{self.interval}"
            for symbol in self.symbols
        ]
        return BINANCE_WS_BASE + "/".join(streams)

    async def start(self):
        backoff = 1
        url = self._build_url()

        self.log.info("Iniciando WebSocket")
        self.log.info("Símbolos: %s", self.symbols)
        self.log.info("Timeframe: %s", self.interval)

        while not self._stop:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    self.ws = ws
                    self.log.info("Conectado a Binance")

                    async for msg in ws:
                        if self._stop:
                            break
                        await self._handle_message(msg)

            except ConnectionClosed:
                self.log.warning("Conexión cerrada, reconectando...")
            except Exception as e:
                self.log.exception("Error WebSocket: %s", e)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

        self.log.info("WebSocket detenido")

    async def _handle_message(self, msg: str):
        data = json.loads(msg).get("data", {})
        if data.get("e") != "kline":
            return

        k = data["k"]

        event = KlineEvent(
            symbol=k["s"],
            interval=k["i"],
            open_time=k["t"],
            close_time=k["T"],
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            is_closed=k["x"],
        )

        # 👉 Recomendado: solo procesar velas cerradas
        if event.is_closed and self.on_kline:
            self.on_kline(event)

    async def stop(self):
        self._stop = True
        if self.ws:
            await self.ws.close()

# ===============================
# Ejemplo de integración con tu bot
# ===============================
def on_kline_closed(event: KlineEvent):
    print(
        f"[CLOSE] {event.symbol} {event.interval} "
        f"O:{event.open} H:{event.high} L:{event.low} "
        f"C:{event.close} V:{event.volume}"
    )
    # 👉 aquí llamarás a tu estrategia:
    # estrategia.evaluar(event)

# ===============================
# Main
# ===============================
async def main():
    ws = BinanceWebSocket(
        symbols=SYMBOLS,
        interval=TIMEFRAME,
        on_kline=on_kline_closed,
    )

    loop = asyncio.get_running_loop()

    def shutdown():
        asyncio.create_task(ws.stop())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown)
        except NotImplementedError:
            pass

    await ws.start()

if __name__ == "__main__":
    asyncio.run(main())
