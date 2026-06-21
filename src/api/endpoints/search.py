"""Symbol search endpoint.

Endpoints:
    GET /api/v1/search?q={query}  — Search symbols and company names
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from ...config.constants import NIFTY_50_SYMBOLS, NIFTY_100_ADDITIONAL
from ...db.connection import db_manager
from ...db.queries import TickerQueries
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(tags=["Search"])

# Built-in fallback universe (used when the instrument DB isn't populated).
_BUILTIN_UNIVERSE = sorted(set(NIFTY_50_SYMBOLS) | set(NIFTY_100_ADDITIONAL))


@router.get(
    "/search",
    summary="Search for stocks",
    description="Search instruments by symbol or company name (case-insensitive fuzzy match).",
)
async def search_instruments(
    q: str = Query(..., min_length=1, max_length=50, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
):
    """Search for instruments by symbol or name."""
    if db_manager.is_connected:
        try:
            session = db_manager.get_session()
            async with session:
                queries = TickerQueries(session)
                results = await queries.search_instruments(q, limit)
            if results:
                return {"status": "success", "source": "timescaledb",
                        "query": q, "count": len(results), "data": results}
        except Exception as e:
            log.warning("search_db_failed", query=q, error=str(e))

    # Fallback: match against the built-in universe.
    ql = q.strip().upper()
    matches = [s for s in _BUILTIN_UNIVERSE if ql in s][:limit]
    return {
        "status": "success",
        "source": "builtin_universe",
        "query": q,
        "count": len(matches),
        "data": [{"symbol": s, "name": s, "exchange": "NSE", "series": "EQ"} for s in matches],
    }
