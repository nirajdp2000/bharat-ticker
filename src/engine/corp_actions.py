"""Conservative corporate-action (split / bonus) back-adjustment for candles.

Upstox/Yahoo historical candles are RAW — a 1:1 bonus or a 5:1 split shows up as
a clean price gap (close halves/quinters) with an inverse volume jump.  Engines
that read raw candles see a phantom crash.  This module detects only
*high-confidence* split/bonus events (price ratio near a common integer factor +
inverse volume jump) and back-adjusts every bar BEFORE the event so the series is
continuous.  Deliberately conservative: when in doubt it leaves the data alone.
"""

from __future__ import annotations

from typing import Any

# Common split / bonus price-shrink factors (post:pre price ratio).
_FACTORS = [2.0, 2.5, 3.0, 4.0, 5.0, 6.0, 10.0]
_PRICE_TOL = 0.06      # ratio must be within 6% of a candidate factor
_VOL_TOL = 0.45        # volume jump must be within 45% of the same factor


def _num(v: Any) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _nearest_factor(ratio: float) -> float | None:
    """Return the split factor a price-shrink ratio matches, else None."""
    if ratio < 1.4:
        return None
    for f in _FACTORS:
        if abs(ratio - f) / f <= _PRICE_TOL:
            return f
    return None


def detect_split_events(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Find high-confidence split/bonus events. candles ascending by time."""
    events: list[dict[str, Any]] = []
    for i in range(1, len(candles)):
        prev_close = _num(candles[i - 1].get("close"))
        cur_open = _num(candles[i].get("open")) or _num(candles[i].get("close"))
        if prev_close <= 0 or cur_open <= 0:
            continue
        ratio = prev_close / cur_open
        factor = _nearest_factor(ratio)
        if not factor:
            continue
        # Confirm with the inverse volume jump (post-split volume scales up ~factor).
        prev_vol = _num(candles[i - 1].get("volume"))
        cur_vol = _num(candles[i].get("volume"))
        vol_ok = True
        if prev_vol > 0 and cur_vol > 0:
            vol_ratio = cur_vol / prev_vol
            vol_ok = abs(vol_ratio - factor) / factor <= max(_VOL_TOL, 0.9)
        if vol_ok:
            events.append({"index": i, "factor": factor,
                           "date": str(candles[i].get("timestamp") or candles[i].get("bucket") or "")})
    return events


def adjust_for_splits(candles: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return (adjusted_candles, events). Non-destructive — returns new dicts.

    For each detected event at index ``i`` with factor ``f``: every bar BEFORE
    ``i`` has its OHLC divided by the cumulative factor and volume multiplied by
    it, bringing old bars onto the current (post-split) price scale.
    """
    if not candles or len(candles) < 3:
        return [dict(c) for c in candles], []

    events = detect_split_events(candles)
    out = [dict(c) for c in candles]
    if not events:
        return out, []

    # Apply newest→oldest so cumulative factors compound correctly for older bars.
    for ev in sorted(events, key=lambda e: e["index"], reverse=True):
        i, f = ev["index"], ev["factor"]
        for j in range(i):
            for k in ("open", "high", "low", "close", "vwap"):
                if out[j].get(k) is not None:
                    out[j][k] = round(_num(out[j][k]) / f, 4)
            if out[j].get("volume") is not None:
                out[j]["volume"] = int(_num(out[j]["volume"]) * f)
    return out, events
