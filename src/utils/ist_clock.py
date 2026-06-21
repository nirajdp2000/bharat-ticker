"""IST (Indian Standard Time) clock utilities for market-hours routing."""

from __future__ import annotations

import datetime
from enum import Enum
from zoneinfo import ZoneInfo

from ..config.holidays import is_trading_holiday, get_holiday_name, get_next_trading_day

IST = ZoneInfo("Asia/Kolkata")


class MarketSession(str, Enum):
    """Possible market sessions throughout the day."""

    PRE_OPEN = "PRE_OPEN"
    CONTINUOUS_TRADING = "CONTINUOUS_TRADING"
    POST_CLOSE = "POST_CLOSE"
    AFTER_MARKET_CLOSE = "AFTER_MARKET_CLOSE"
    CLOSED = "CLOSED"
    HOLIDAY = "HOLIDAY"
    WEEKEND = "WEEKEND"


# ── Session Boundaries (IST) ────────────────────────────────────────────────
_PRE_OPEN_START = datetime.time(9, 0, 0)
_MARKET_OPEN = datetime.time(9, 15, 0)
_MARKET_CLOSE = datetime.time(15, 30, 0)
_POST_CLOSE_END = datetime.time(15, 40, 0)
_AMC_END = datetime.time(16, 0, 0)


def now_ist() -> datetime.datetime:
    """Get current datetime in IST."""
    return datetime.datetime.now(tz=IST)


def today_ist() -> datetime.date:
    """Get today's date in IST."""
    return now_ist().date()


def get_market_session(dt: datetime.datetime | None = None) -> MarketSession:
    """Determine the current market session based on IST datetime.

    Parameters
    ----------
    dt : datetime, optional
        The datetime to check.  Defaults to now (IST).

    Returns
    -------
    MarketSession
        The current session enum value.
    """
    if dt is None:
        dt = now_ist()
    else:
        # Ensure we're working in IST
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=IST)
        else:
            dt = dt.astimezone(IST)

    date = dt.date()
    time = dt.time()

    # Weekend check
    if date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return MarketSession.WEEKEND

    # Holiday check
    if is_trading_holiday(date):
        return MarketSession.HOLIDAY

    # Session determination
    if time < _PRE_OPEN_START:
        return MarketSession.CLOSED
    elif _PRE_OPEN_START <= time < _MARKET_OPEN:
        return MarketSession.PRE_OPEN
    elif _MARKET_OPEN <= time < _MARKET_CLOSE:
        return MarketSession.CONTINUOUS_TRADING
    elif _MARKET_CLOSE <= time < _POST_CLOSE_END:
        return MarketSession.POST_CLOSE
    elif _POST_CLOSE_END <= time < _AMC_END:
        return MarketSession.AFTER_MARKET_CLOSE
    else:
        return MarketSession.CLOSED


def is_market_open(dt: datetime.datetime | None = None) -> bool:
    """Check if the market is currently in a trading session (pre-open, continuous, or post-close)."""
    session = get_market_session(dt)
    return session in {
        MarketSession.PRE_OPEN,
        MarketSession.CONTINUOUS_TRADING,
        MarketSession.POST_CLOSE,
    }


def is_trading_hours(dt: datetime.datetime | None = None) -> bool:
    """Check if we're in the main continuous trading session (9:15-15:30)."""
    session = get_market_session(dt)
    return session == MarketSession.CONTINUOUS_TRADING


def should_scrape(dt: datetime.datetime | None = None) -> bool:
    """Determine if the scraping engine should be actively fetching.

    Returns True during pre-open, continuous, and post-close sessions.
    """
    return is_market_open(dt)


def get_next_market_open(dt: datetime.datetime | None = None) -> datetime.datetime:
    """Get the datetime of the next market open (9:15 AM IST)."""
    if dt is None:
        dt = now_ist()

    date = dt.date()

    # If before market open today and today is a trading day
    if dt.time() < _MARKET_OPEN and date.weekday() < 5 and not is_trading_holiday(date):
        return datetime.datetime.combine(date, _MARKET_OPEN, tzinfo=IST)

    # Otherwise, next trading day
    next_day = get_next_trading_day(date)
    return datetime.datetime.combine(next_day, _MARKET_OPEN, tzinfo=IST)


def get_market_close_today() -> datetime.datetime:
    """Get today's market close time (3:30 PM IST)."""
    return datetime.datetime.combine(today_ist(), _MARKET_CLOSE, tzinfo=IST)


def get_market_state_dict(dt: datetime.datetime | None = None) -> dict:
    """Build the MarketState dictionary for API responses."""
    if dt is None:
        dt = now_ist()

    session = get_market_session(dt)
    date = dt.date()
    holiday_name = get_holiday_name(date)

    status_map = {
        MarketSession.PRE_OPEN: "PRE_OPEN",
        MarketSession.CONTINUOUS_TRADING: "OPEN",
        MarketSession.POST_CLOSE: "POST_CLOSE",
        MarketSession.AFTER_MARKET_CLOSE: "CLOSED",
        MarketSession.CLOSED: "CLOSED",
        MarketSession.HOLIDAY: "CLOSED",
        MarketSession.WEEKEND: "CLOSED",
    }

    return {
        "status": status_map[session],
        "session": session.value,
        "exchange_time": dt.isoformat(),
        "next_open": get_next_market_open(dt).isoformat() if not is_market_open(dt) else None,
        "next_close": get_market_close_today().isoformat() if is_market_open(dt) else None,
        "is_holiday": session == MarketSession.HOLIDAY,
        "holiday_name": holiday_name,
    }
