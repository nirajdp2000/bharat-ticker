"""Optimized TimescaleDB queries for tick and OHLCV data."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from ..utils.logger import get_logger

log = get_logger(__name__)


# ── Raw SQL Queries ──────────────────────────────────────────────────────────
# Using raw SQL for maximum performance with TimescaleDB-specific features.

INSERT_TICK = sa.text("""
    INSERT INTO ticks (time, symbol, exchange, ltp, open, high, low, close,
                       volume, value, vwap, total_buy_qty, total_sell_qty,
                       source, source_latency)
    VALUES (:time, :symbol, :exchange, :ltp, :open, :high, :low, :close,
            :volume, :value, :vwap, :total_buy_qty, :total_sell_qty,
            :source, :source_latency)
""")

INSERT_EOD = sa.text("""
    INSERT INTO eod_data (date, symbol, exchange, open, high, low, close,
                          vwap, volume, value, delivery_qty, delivery_pct, trades)
    VALUES (:date, :symbol, :exchange, :open, :high, :low, :close,
            :vwap, :volume, :value, :delivery_qty, :delivery_pct, :trades)
    ON CONFLICT (date, symbol, exchange) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        vwap = EXCLUDED.vwap,
        volume = EXCLUDED.volume,
        value = EXCLUDED.value,
        delivery_qty = EXCLUDED.delivery_qty,
        delivery_pct = EXCLUDED.delivery_pct,
        trades = EXCLUDED.trades
""")

GET_LATEST_EOD = sa.text("""
    SELECT * FROM eod_data
    WHERE symbol = :symbol AND exchange = :exchange
    ORDER BY date DESC
    LIMIT 1
""")

GET_OHLCV_CANDLES = sa.text("""
    SELECT
        time_bucket(:interval, time) AS bucket,
        symbol,
        exchange,
        first(ltp, time)  AS open,
        max(ltp)           AS high,
        min(ltp)           AS low,
        last(ltp, time)    AS close,
        sum(volume)        AS volume,
        sum(value)         AS value,
        last(vwap, time)   AS vwap
    FROM ticks
    WHERE symbol = :symbol
      AND exchange = :exchange
      AND time >= :start_time
      AND time <= :end_time
    GROUP BY bucket, symbol, exchange
    ORDER BY bucket ASC
""")

GET_EOD_RANGE = sa.text("""
    SELECT * FROM eod_data
    WHERE symbol = :symbol
      AND exchange = :exchange
      AND date >= :start_date
      AND date <= :end_date
    ORDER BY date ASC
""")

SEARCH_INSTRUMENTS = sa.text("""
    SELECT symbol, name, exchange, series, sector, is_active
    FROM instruments
    WHERE (symbol ILIKE :query OR name ILIKE :query)
      AND is_active = true
    ORDER BY symbol ASC
    LIMIT :limit
""")

UPSERT_INSTRUMENT = sa.text("""
    INSERT INTO instruments (symbol, isin, name, exchange, series, sector,
                             industry, lot_size, face_value, listing_date, is_active, updated_at)
    VALUES (:symbol, :isin, :name, :exchange, :series, :sector,
            :industry, :lot_size, :face_value, :listing_date, :is_active, NOW())
    ON CONFLICT (symbol) DO UPDATE SET
        isin = EXCLUDED.isin,
        name = EXCLUDED.name,
        exchange = EXCLUDED.exchange,
        series = EXCLUDED.series,
        sector = EXCLUDED.sector,
        industry = EXCLUDED.industry,
        lot_size = EXCLUDED.lot_size,
        face_value = EXCLUDED.face_value,
        is_active = EXCLUDED.is_active,
        updated_at = NOW()
""")


# ── Query Executor ───────────────────────────────────────────────────────────

class TickerQueries:
    """Executes optimized queries against TimescaleDB."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def insert_tick(self, tick_data: dict[str, Any]) -> None:
        """Insert a single tick into the hypertable."""
        await self._session.execute(INSERT_TICK, tick_data)

    async def insert_ticks_bulk(self, ticks: list[dict[str, Any]]) -> None:
        """Bulk insert ticks for maximum throughput."""
        if not ticks:
            return
        # Use executemany for batch insert
        await self._session.execute(INSERT_TICK, ticks)
        await self._session.commit()

    async def upsert_eod(self, eod_data: dict[str, Any]) -> None:
        """Insert or update an EOD record."""
        await self._session.execute(INSERT_EOD, eod_data)
        await self._session.commit()

    async def get_latest_eod(
        self, symbol: str, exchange: str = "NSE"
    ) -> dict[str, Any] | None:
        """Get the most recent EOD data for a symbol."""
        result = await self._session.execute(
            GET_LATEST_EOD, {"symbol": symbol, "exchange": exchange}
        )
        row = result.mappings().first()
        return dict(row) if row else None

    async def get_ohlcv_candles(
        self,
        symbol: str,
        exchange: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
    ) -> list[dict[str, Any]]:
        """Get OHLCV candles for a symbol using TimescaleDB time_bucket."""
        result = await self._session.execute(
            GET_OHLCV_CANDLES,
            {
                "symbol": symbol,
                "exchange": exchange,
                "interval": interval,
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        return [dict(row) for row in result.mappings().all()]

    async def get_eod_range(
        self,
        symbol: str,
        exchange: str,
        start_date: date,
        end_date: date,
    ) -> list[dict[str, Any]]:
        """Get EOD data for a date range."""
        result = await self._session.execute(
            GET_EOD_RANGE,
            {
                "symbol": symbol,
                "exchange": exchange,
                "start_date": start_date,
                "end_date": end_date,
            },
        )
        return [dict(row) for row in result.mappings().all()]

    async def search_instruments(
        self, query: str, limit: int = 20
    ) -> list[dict[str, Any]]:
        """Search instruments by symbol or name (case-insensitive)."""
        result = await self._session.execute(
            SEARCH_INSTRUMENTS,
            {"query": f"%{query}%", "limit": limit},
        )
        return [dict(row) for row in result.mappings().all()]

    async def upsert_instrument(self, instrument: dict[str, Any]) -> None:
        """Insert or update an instrument in the master table."""
        await self._session.execute(UPSERT_INSTRUMENT, instrument)
        await self._session.commit()
