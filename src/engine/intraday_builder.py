"""Aggregate raw intraday price/volume points into OHLC candles.

Used to turn the BSE StockReachGraph minute-resolution curve (real exchange
data) into proper N-minute OHLCV bars for any timeframe (1m/5m/15m/30m), so the
intraday feed is REAL — never the 15-min-delayed Yahoo chart.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

# superbrain/Upstox interval name → bucket seconds
INTRADAY_SECONDS = {
    "1minute": 60, "1m": 60,
    "2minute": 120, "2m": 120,
    "3minute": 180, "3m": 180,
    "5minute": 300, "5m": 300,
    "10minute": 600, "10m": 600,
    "15minute": 900, "15m": 900,
    "30minute": 1800, "30m": 1800,
    "60minute": 3600, "60m": 3600, "1h": 3600,
}


def _epoch(ts: str) -> float | None:
    try:
        return datetime.fromisoformat(ts).timestamp()
    except (ValueError, TypeError):
        return None


def points_to_ohlc(points: list[dict[str, Any]], bucket_seconds: int,
                   limit: int = 400) -> list[dict[str, Any]]:
    """points = [{timestamp(iso), price, volume(per-interval)}] ascending."""
    if not points or bucket_seconds <= 0:
        return []
    buckets: dict[int, dict[str, Any]] = {}
    order: list[int] = []
    for p in points:
        e = _epoch(p.get("timestamp", ""))
        price = p.get("price")
        if e is None or price is None or price <= 0:
            continue
        b = int(e // bucket_seconds) * bucket_seconds
        cell = buckets.get(b)
        if cell is None:
            cell = {"o": price, "h": price, "l": price, "c": price, "v": 0.0,
                    "iso": p.get("timestamp")}
            buckets[b] = cell
            order.append(b)
        cell["h"] = max(cell["h"], price)
        cell["l"] = min(cell["l"], price)
        cell["c"] = price
        cell["v"] += float(p.get("volume") or 0)

    out: list[dict[str, Any]] = []
    for b in order[-limit:]:
        c = buckets[b]
        out.append({
            "timestamp": datetime.fromtimestamp(b, tz=datetime.fromisoformat(c["iso"]).tzinfo).isoformat(),
            "open": round(c["o"], 2), "high": round(c["h"], 2),
            "low": round(c["l"], 2), "close": round(c["c"], 2),
            "volume": int(c["v"]), "oi": 0,
        })
    return out
