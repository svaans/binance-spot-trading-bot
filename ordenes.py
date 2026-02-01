# ordenes.py
from __future__ import annotations

import hashlib
import hmac
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

from db import DB

load_dotenv("claves.env")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
KILL_SWITCH = os.getenv("KILL_SWITCH", "false").strip().lower() == "true"

BASE_URL = "https://api.binance.com"
TIMEOUT = 15


@dataclass(frozen=True)
class OrderResult:
    symbol: str
    side: str
    order_type: str
    order_id: int | None
    client_order_id: str
    status: str | None
    transact_time: int | None
    raw: Dict[str, Any]


class BinanceSpotExecutor:
    """
    Executor robusto:
    - cache de filtros por símbolo (exchangeInfo)
    - redondeo a stepSize (LOT_SIZE)
    - validación MIN_NOTIONAL/NOTIONAL usando el último precio
    """

    def __init__(self, db: DB, api_key: str = BINANCE_API_KEY, api_secret: str = BINANCE_API_SECRET) -> None:
        if not api_key or not api_secret:
            raise RuntimeError("Faltan BINANCE_API_KEY o BINANCE_API_SECRET en claves.env")

        self.db = db
        self.api_key = api_key
        self.api_secret = api_secret
        self.log = logging.getLogger("ORDENES")

        self.session = requests.Session()
        self.session.headers.update({"X-MBX-APIKEY": self.api_key})

        self._filters_cache: Dict[str, Dict[str, Any]] = {}

    # ---------- HTTP ----------
    def _sign(self, params: Dict[str, Any]) -> str:
        query = urlencode(params, doseq=True)
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _private(self, method: str, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if params is None:
            params = {}
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._sign(params)

        url = BASE_URL + path
        if method.upper() == "GET":
            r = self.session.get(url, params=params, timeout=TIMEOUT)
        elif method.upper() == "POST":
            r = self.session.post(url, params=params, timeout=TIMEOUT)
        else:
            raise ValueError("Método no soportado")

        data = r.json()
        if r.status_code >= 400:
            raise RuntimeError(f"Binance error {r.status_code}: {data}")
        return data

    def _public(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        r = requests.get(BASE_URL + path, params=params or {}, timeout=TIMEOUT)
        r.raise_for_status()
        return r.json()

    # ---------- Market data ----------
    def get_last_price(self, symbol: str) -> float:
        data = self._public("/api/v3/ticker/price", {"symbol": symbol.upper()})
        return float(data["price"])

    # ---------- Balances ----------
    def get_account(self) -> Dict[str, Any]:
        return self._private("GET", "/api/v3/account")

    def get_free_balance(self, asset: str) -> float:
        acc = self.get_account()
        for b in acc.get("balances", []):
            if b.get("asset") == asset:
                return float(b.get("free", "0"))
        return 0.0

    def compute_quote_budget(self, quote_asset: str, risk_pct: float) -> float:
        return max(0.0, self.get_free_balance(quote_asset) * float(risk_pct))

    # ---------- Filters ----------
    def get_symbol_filters(self, symbol: str) -> Dict[str, Any]:
        sym = symbol.upper()
        if sym in self._filters_cache:
            return self._filters_cache[sym]

        info = self._public("/api/v3/exchangeInfo", {"symbol": sym})
        s = info["symbols"][0]
        filters = {f["filterType"]: f for f in s["filters"]}
        self._filters_cache[sym] = filters
        return filters

    @staticmethod
    def _floor_to_step(qty: float, step: float) -> float:
        if step <= 0:
            return qty
        return math.floor(qty / step) * step

    def normalize_base_qty(self, symbol: str, base_qty: float) -> float:
        """
        Ajusta quantity a LOT_SIZE.stepSize y valida minQty.
        """
        filters = self.get_symbol_filters(symbol)
        lot = filters.get("LOT_SIZE")
        if not lot:
            return base_qty

        step = float(lot["stepSize"])
        min_qty = float(lot["minQty"])

        q = self._floor_to_step(base_qty, step)
        # Redondeo final para evitar 0.30000000004
        q = float(f"{q:.12f}")

        if q < min_qty:
            return 0.0
        return q

    def check_min_notional(self, symbol: str, base_qty: float, price: Optional[float] = None) -> Tuple[bool, float]:
        """
        Devuelve (ok, notional). notional = base_qty * price.
        """
        if price is None:
            price = self.get_last_price(symbol)

        notional = base_qty * price

        filters = self.get_symbol_filters(symbol)
        # Binance usa a veces MIN_NOTIONAL o NOTIONAL (según símbolo/mercado)
        mn = filters.get("MIN_NOTIONAL")
        nt = filters.get("NOTIONAL")

        min_required = None
        if mn and "minNotional" in mn:
            min_required = float(mn["minNotional"])
        elif nt and "minNotional" in nt:
            min_required = float(nt["minNotional"])

        # Si no hay filtro, asumimos ok
        if min_required is None:
            return True, notional

        return (notional >= min_required), notional

    # ---------- Orders ----------
    def place_market_buy_by_quote(self, symbol: str, quote_order_qty: float) -> OrderResult:
        """
        BUY MARKET gastando quote (EUR). No requiere stepSize.
        Aun así, Binance puede rechazar si es demasiado pequeño.
        """
        if KILL_SWITCH:
            raise RuntimeError("KILL_SWITCH activado: no se ejecutan órdenes.")

        client_id = self._client_id(symbol, "BUY")
        params = {
            "symbol": symbol.upper(),
            "side": "BUY",
            "type": "MARKET",
            "quoteOrderQty": self._fmt(quote_order_qty),
            "newClientOrderId": client_id,
        }
        data = self._private("POST", "/api/v3/order", params)

        res = self._to_result(data, client_id)
        self.db.log_order(
            symbol=res.symbol, side=res.side, order_type=res.order_type,
            client_order_id=res.client_order_id, order_id=res.order_id,
            status=res.status, raw=res.raw
        )
        return res

    def place_market_sell_by_base_safe(self, symbol: str, base_qty: float) -> OrderResult:
        """
        SELL MARKET seguro:
        - ajusta a stepSize
        - valida minQty
        - valida MIN_NOTIONAL usando last price
        """
        if KILL_SWITCH:
            raise RuntimeError("KILL_SWITCH activado: no se ejecutan órdenes.")

        qty = self.normalize_base_qty(symbol, base_qty)
        if qty <= 0:
            raise RuntimeError(f"Cantidad base inválida tras LOT_SIZE para {symbol}: {base_qty}")

        ok, notional = self.check_min_notional(symbol, qty)
        if not ok:
            raise RuntimeError(f"MIN_NOTIONAL/NOTIONAL no alcanzado en {symbol}. notional={notional:.4f}")

        client_id = self._client_id(symbol, "SELL")
        params = {
            "symbol": symbol.upper(),
            "side": "SELL",
            "type": "MARKET",
            "quantity": self._fmt(qty),
            "newClientOrderId": client_id,
        }
        data = self._private("POST", "/api/v3/order", params)

        res = self._to_result(data, client_id)
        self.db.log_order(
            symbol=res.symbol, side=res.side, order_type=res.order_type,
            client_order_id=res.client_order_id, order_id=res.order_id,
            status=res.status, raw=res.raw
        )
        return res

    # ---------- Utils ----------
    @staticmethod
    def _fmt(x: float) -> str:
        return f"{x:.12f}".rstrip("0").rstrip(".")

    @staticmethod
    def _client_id(symbol: str, side: str) -> str:
        return f"bot_{symbol.upper()}_{side}_{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _to_result(data: Dict[str, Any], client_id: str) -> OrderResult:
        return OrderResult(
            symbol=str(data.get("symbol", "")),
            side=str(data.get("side", "")),
            order_type=str(data.get("type", "")),
            order_id=int(data["orderId"]) if "orderId" in data else None,
            client_order_id=str(data.get("clientOrderId", client_id)),
            status=str(data.get("status")) if "status" in data else None,
            transact_time=int(data["transactTime"]) if "transactTime" in data else None,
            raw=data,
        )




