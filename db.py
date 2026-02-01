# db.py
import json
import os
import logging
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv("claves.env")
log = logging.getLogger("DB")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError("Falta DATABASE_URL en claves.env")

# Si tu DATABASE_URL ya trae ?sslmode=require, esto lo refuerza.
CONNECT_KWARGS = {
    "connect_timeout": 10,
    "sslmode": "require",
    "keepalives": 1,
    "keepalives_idle": 30,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}

class DB:
    def __init__(self, dsn: str = DATABASE_URL):
        self.dsn = dsn

    def _conn(self):
        # psycopg2 acepta query params en DSN, pero aquí forzamos ssl/keepalive
        return psycopg2.connect(self.dsn, **CONNECT_KWARGS)

    def init_schema(self):
        ddl = """
        CREATE TABLE IF NOT EXISTS positions (
          symbol TEXT PRIMARY KEY,
          base_asset TEXT NOT NULL,
          quote_asset TEXT NOT NULL,
          entry_price DOUBLE PRECISION NOT NULL,
          status TEXT NOT NULL DEFAULT 'OPEN',
          opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          closed_at TIMESTAMPTZ NULL
        );

        CREATE TABLE IF NOT EXISTS orders (
          id BIGSERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          side TEXT NOT NULL,
          order_type TEXT NOT NULL,
          client_order_id TEXT NOT NULL,
          order_id BIGINT NULL,
          status TEXT NULL,
          raw_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_orders_symbol_created
        ON orders(symbol, created_at DESC);

        CREATE INDEX IF NOT EXISTS idx_positions_status
        ON positions(status);

        CREATE TABLE IF NOT EXISTS indicators_snapshot (
          id BIGSERIAL PRIMARY KEY,
          symbol TEXT NOT NULL,
          interval TEXT NOT NULL,
          close_time BIGINT NOT NULL,
          payload_json JSONB NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE UNIQUE INDEX IF NOT EXISTS uq_indicators_snapshot_symbol_interval_close
        ON indicators_snapshot(symbol, interval, close_time);

        CREATE TABLE IF NOT EXISTS taapi_call_log (
          call_date DATE PRIMARY KEY,
          calls_count INTEGER NOT NULL DEFAULT 0,
          updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(ddl)
        log.info("Schema OK")

    def log_order(self, *, symbol: str, side: str, order_type: str,
                  client_order_id: str, order_id: int | None,
                  status: str | None, raw: Dict[str, Any]) -> None:
        sql = """
        INSERT INTO orders(symbol, side, order_type, client_order_id, order_id, status, raw_json)
        VALUES (%s,%s,%s,%s,%s,%s,%s::jsonb)
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (
                symbol, side, order_type, client_order_id,
                order_id, status, json.dumps(raw)
            ))

    def upsert_open_position(self, *, symbol: str, base_asset: str, quote_asset: str, entry_price: float) -> None:
        sql = """
        INSERT INTO positions(symbol, base_asset, quote_asset, entry_price, status)
        VALUES (%s,%s,%s,%s,'OPEN')
        ON CONFLICT (symbol) DO UPDATE
          SET base_asset = EXCLUDED.base_asset,
              quote_asset = EXCLUDED.quote_asset,
              entry_price = EXCLUDED.entry_price,
              status = 'OPEN',
              opened_at = NOW(),
              closed_at = NULL
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (symbol, base_asset, quote_asset, entry_price))

    def close_position(self, *, symbol: str) -> None:
        sql = """
        UPDATE positions
           SET status='CLOSED', closed_at=NOW()
         WHERE symbol=%s AND status='OPEN'
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (symbol,))

    def get_open_positions(self) -> List[Dict[str, Any]]:
        sql = "SELECT symbol, base_asset, quote_asset, entry_price FROM positions WHERE status='OPEN'"
        with self._conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            return list(cur.fetchall())
        
    def get_indicators_snapshot(self, *, symbol: str, interval: str, close_time: int) -> Dict[str, Any] | None:
        sql = """
        SELECT payload_json
          FROM indicators_snapshot
         WHERE symbol=%s AND interval=%s AND close_time=%s
         LIMIT 1
        """
        with self._conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, (symbol, interval, close_time))
            row = cur.fetchone()
            if not row:
                return None
            return row["payload_json"]

    def insert_indicators_snapshot(
        self,
        *,
        symbol: str,
        interval: str,
        close_time: int,
        payload_json: Dict[str, Any],
    ) -> None:
        sql = """
        INSERT INTO indicators_snapshot(symbol, interval, close_time, payload_json)
        VALUES (%s, %s, %s, %s::jsonb)
        ON CONFLICT (symbol, interval, close_time) DO NOTHING
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (symbol, interval, close_time, json.dumps(payload_json)))

    def increment_taapi_call(self, *, call_date: str) -> None:
        sql = """
        INSERT INTO taapi_call_log(call_date, calls_count)
        VALUES (%s, 1)
        ON CONFLICT (call_date) DO UPDATE
          SET calls_count = taapi_call_log.calls_count + 1,
              updated_at = NOW()
        """
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (call_date,))

    def get_taapi_calls_for_date(self, *, call_date: str) -> int:
        sql = "SELECT calls_count FROM taapi_call_log WHERE call_date=%s"
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (call_date,))
            row = cur.fetchone()
            if not row:
                return 0
            return int(row[0])

