"""Smart market-hours routing middleware.

Injects market state into every request and logs routing decisions.
The actual routing logic (Redis vs TimescaleDB) is handled by each
endpoint, but this middleware provides the context.
"""

from __future__ import annotations

import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from ...utils.ist_clock import get_market_session, is_market_open, now_ist
from ...utils.logger import get_logger

log = get_logger(__name__)


class MarketHoursMiddleware(BaseHTTPMiddleware):
    """Middleware that injects market state headers into every response."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.time()

        # Determine market state
        session = get_market_session()
        market_open = is_market_open()

        # Store in request state for endpoints to use
        request.state.market_session = session
        request.state.market_open = market_open

        # Process request
        response = await call_next(request)

        # Add market state headers
        elapsed_ms = (time.time() - start) * 1000
        response.headers["X-Market-Session"] = session.value
        response.headers["X-Market-Open"] = str(market_open).lower()
        response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.1f}"
        response.headers["X-Server-Time-IST"] = now_ist().isoformat()

        return response
