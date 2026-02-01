import logging
import os
from typing import Any, Dict, List

import requests

from db import DB


log = logging.getLogger("TAAPI")

DEFAULT_ENDPOINT = "https://api.taapi.io/bulk"
DEFAULT_EXCHANGE = "binance"


class TAAPIClient:
    def __init__(
        self,
        db: DB,
        *,
        secret: str | None = None,
        endpoint: str = DEFAULT_ENDPOINT,
        exchange: str = DEFAULT_EXCHANGE,
        timeout_s: int = 15,
    ) -> None:
        self.db = db
        self.secret = (secret or os.getenv("TAAPI_SECRET", "")).strip()
        if not self.secret:
            raise RuntimeError("Falta TAAPI_SECRET en variables de entorno")
        self.endpoint = endpoint
        self.exchange = exchange
        self.timeout_s = timeout_s

    def _indicator(
        self,
        *,
        indicator: str,
        indicator_id: str,
        symbol: str,
        interval: str,
        **params: Any,
    ) -> Dict[str, Any]:
        payload = {
            "id": indicator_id,
            "indicator": indicator,
            "exchange": self.exchange,
            "symbol": symbol,
            "interval": interval,
        }
        payload.update(params)
        return payload

    def _build_construct(self, symbol: str, interval: str) -> List[Dict[str, Any]]:
        return [
            self._indicator(indicator="ema", indicator_id="ema_9", symbol=symbol, interval=interval, period=9),
            self._indicator(indicator="ema", indicator_id="ema_20", symbol=symbol, interval=interval, period=20),
            self._indicator(indicator="ema", indicator_id="ema_50", symbol=symbol, interval=interval, period=50),
            self._indicator(indicator="ema", indicator_id="ema_100", symbol=symbol, interval=interval, period=100),
            self._indicator(indicator="ema", indicator_id="ema_200", symbol=symbol, interval=interval, period=200),
            self._indicator(indicator="sma", indicator_id="sma_20", symbol=symbol, interval=interval, period=20),
            self._indicator(indicator="sma", indicator_id="sma_50", symbol=symbol, interval=interval, period=50),
            self._indicator(indicator="sma", indicator_id="sma_200", symbol=symbol, interval=interval, period=200),
            self._indicator(indicator="macd", indicator_id="macd", symbol=symbol, interval=interval),
            self._indicator(indicator="adx", indicator_id="adx", symbol=symbol, interval=interval, period=14),
            self._indicator(indicator="ichimoku", indicator_id="ichimoku", symbol=symbol, interval=interval),
            self._indicator(indicator="psar", indicator_id="psar", symbol=symbol, interval=interval),
            self._indicator(indicator="rsi", indicator_id="rsi", symbol=symbol, interval=interval, period=14),
            self._indicator(indicator="stoch", indicator_id="stoch", symbol=symbol, interval=interval),
            self._indicator(indicator="stochrsi", indicator_id="stochrsi", symbol=symbol, interval=interval),
            self._indicator(indicator="cci", indicator_id="cci", symbol=symbol, interval=interval, period=20),
            self._indicator(indicator="willr", indicator_id="willr", symbol=symbol, interval=interval, period=14),
            self._indicator(indicator="roc", indicator_id="roc", symbol=symbol, interval=interval, period=12),
            self._indicator(indicator="mom", indicator_id="momentum", symbol=symbol, interval=interval, period=10),
            self._indicator(indicator="atr", indicator_id="atr", symbol=symbol, interval=interval, period=14),
            self._indicator(indicator="bbands", indicator_id="bbands", symbol=symbol, interval=interval),
            self._indicator(indicator="keltner", indicator_id="keltner", symbol=symbol, interval=interval),
            self._indicator(indicator="donchian", indicator_id="donchian", symbol=symbol, interval=interval, period=20),
            self._indicator(indicator="obv", indicator_id="obv", symbol=symbol, interval=interval),
            self._indicator(indicator="mfi", indicator_id="mfi", symbol=symbol, interval=interval, period=14),
            self._indicator(indicator="vwap", indicator_id="vwap", symbol=symbol, interval=interval),
            self._indicator(indicator="pivotpoints", indicator_id="pivotpoints", symbol=symbol, interval=interval),
        ]

    def _normalize_response(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        indicators: Dict[str, Any] = {}
        data = payload.get("data", [])
        for item in data:
            item_id = item.get("id")
            if not item_id:
                continue
            if "result" in item:
                indicators[item_id] = item["result"]
            elif "value" in item:
                indicators[item_id] = {"value": item["value"]}
            else:
                indicators[item_id] = item
        return indicators

    def fetch_snapshot(self, *, symbol: str, interval: str, close_time: int) -> Dict[str, Any]:
        cached = self.db.get_indicators_snapshot(symbol=symbol, interval=interval, close_time=close_time)
        if cached:
            normalized = self._normalize_response(cached)
            return {
                "symbol": symbol,
                "interval": interval,
                "close_time": close_time,
                "indicators": normalized,
                "source": "cache",
            }

        payload = {
            "secret": self.secret,
            "construct": self._build_construct(symbol=symbol, interval=interval),
        }

        try:
            response = requests.post(self.endpoint, json=payload, timeout=self.timeout_s)
        except requests.exceptions.Timeout as exc:
            log.warning("Timeout consultando TAAPI para %s %s", symbol, interval)
            raise RuntimeError("Timeout consultando TAAPI") from exc
        except requests.exceptions.RequestException as exc:
            log.warning("Error de red consultando TAAPI para %s %s", symbol, interval)
            raise RuntimeError("Error de red consultando TAAPI") from exc

        if response.status_code == 429:
            raise RuntimeError("Rate limit alcanzado en TAAPI")

        if not response.ok:
            raise RuntimeError(f"TAAPI respondió con estado {response.status_code}")

        try:
            payload_json = response.json()
        except ValueError as exc:
            raise RuntimeError("Respuesta inválida de TAAPI (JSON)") from exc

        if "data" not in payload_json:
            raise RuntimeError("Respuesta inválida de TAAPI (sin data)")

        self.db.insert_indicators_snapshot(
            symbol=symbol,
            interval=interval,
            close_time=close_time,
            payload_json=payload_json,
        )

        normalized = self._normalize_response(payload_json)
        return {
            "symbol": symbol,
            "interval": interval,
            "close_time": close_time,
            "indicators": normalized,
            "source": "taapi",
        }