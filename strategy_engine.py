from __future__ import annotations

import inspect
import logging
import os
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional

import pandas as pd


log = logging.getLogger("STRATEGY_ENGINE")


EXCLUDE_PREFIX = ("__", "loader", "gestor", "analisis", "validacion", "validador")


@dataclass(frozen=True)
class StrategyResult:
    name: str
    active: bool
    message: str


class StrategyEngine:
    def __init__(
        self,
        *,
        entry_dir: str,
        exit_dir: str,
        max_candles: int = 200,
    ) -> None:
        self.entry_dir = entry_dir
        self.exit_dir = exit_dir
        self.max_candles = max(20, max_candles)
        self._candles: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=self.max_candles)
        )
        self.entry_strategies = self._load_strategies(entry_dir, prefix="estrategia_")
        self.exit_strategies = self._load_strategies(exit_dir, prefix="salida_")

        log.info(
            "Estrategias cargadas: %d entradas, %d salidas",
            len(self.entry_strategies),
            len(self.exit_strategies),
        )

    def update_candle(self, *, symbol: str, candle: Dict[str, Any]) -> pd.DataFrame:
        self._candles[symbol].append(candle)
        return self._to_dataframe(symbol)

    def _to_dataframe(self, symbol: str) -> pd.DataFrame:
        data = list(self._candles[symbol])
        return pd.DataFrame(data)

    def evaluate_entries(
        self,
        *,
        df: pd.DataFrame,
        indicators: Optional[Dict[str, Any]] = None,
    ) -> List[StrategyResult]:
        return self._evaluate(self.entry_strategies, df=df, indicators=indicators)

    def evaluate_exits(
        self,
        *,
        df: pd.DataFrame,
        indicators: Optional[Dict[str, Any]] = None,
    ) -> List[StrategyResult]:
        return self._evaluate(self.exit_strategies, df=df, indicators=indicators)

    def _evaluate(
        self,
        strategies: Dict[str, Callable[..., Any]],
        *,
        df: pd.DataFrame,
        indicators: Optional[Dict[str, Any]],
    ) -> List[StrategyResult]:
        resultados: List[StrategyResult] = []
        for name, func in strategies.items():
            result = self._run_strategy(func, df=df, indicators=indicators)
            if result is None:
                continue
            resultados.append(StrategyResult(name=name, active=result["activo"], message=result["mensaje"]))
        return resultados

    def _run_strategy(
        self,
        func: Callable[..., Any],
        *,
        df: pd.DataFrame,
        indicators: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        try:
            payload = self._build_call_payload(func, df=df, indicators=indicators)
            response = func(*payload["args"], **payload["kwargs"])
        except Exception as exc:
            log.warning("Estrategia %s falló: %s", getattr(func, "__name__", func), exc)
            return None

        if isinstance(response, dict) and "activo" in response:
            mensaje = response.get("mensaje", "")
            return {"activo": bool(response["activo"]), "mensaje": mensaje}

        return None

    def _build_call_payload(
        self,
        func: Callable[..., Any],
        *,
        df: pd.DataFrame,
        indicators: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        signature = inspect.signature(func)
        params = list(signature.parameters.values())

        if not params:
            return {"args": [], "kwargs": {}}

        kwargs: Dict[str, Any] = {}
        param_names = {param.name for param in params}
        if "df" in param_names or "dataframe" in param_names or "data" in param_names:
            kwargs[[name for name in param_names if name in {"df", "dataframe", "data"}][0]] = df
        if indicators is not None and (
            "indicators" in param_names or "indicadores" in param_names or "snapshot" in param_names
        ):
            key = [name for name in param_names if name in {"indicators", "indicadores", "snapshot"}][0]
            kwargs[key] = indicators

        if kwargs:
            return {"args": [], "kwargs": kwargs}

        args: List[Any] = [df]
        if indicators is not None and len(params) >= 2:
            args.append(indicators)
        return {"args": args, "kwargs": {}}

    def _load_strategies(self, carpeta: str, *, prefix: str) -> Dict[str, Callable[..., Any]]:
        strategies: Dict[str, Callable[..., Any]] = {}
        if not os.path.isdir(carpeta):
            log.warning("Carpeta no encontrada: %s", carpeta)
            return strategies

        for archivo in os.listdir(carpeta):
            if not archivo.endswith(".py"):
                continue
            if archivo.startswith(EXCLUDE_PREFIX):
                continue
            if not archivo.startswith(prefix):
                continue

            nombre_modulo = archivo[:-3]
            ruta = os.path.join(carpeta, archivo)
            func = self._load_function_from_file(ruta, nombre_modulo)
            if func is None:
                continue
            strategies[nombre_modulo] = func

        return strategies

    def _load_function_from_file(self, ruta: str, nombre_funcion: str) -> Optional[Callable[..., Any]]:
        import importlib.util

        spec = importlib.util.spec_from_file_location(nombre_funcion, ruta)
        if spec is None or spec.loader is None:
            log.warning("Spec inválida para %s", ruta)
            return None

        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
        except Exception as exc:
            log.warning("No se pudo cargar %s: %s", ruta, exc)
            return None

        func = getattr(module, nombre_funcion, None)
        if not callable(func):
            log.warning("No se encontró función %s en %s", nombre_funcion, ruta)
            return None
        return func