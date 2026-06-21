"""Unit tests for the IST clock utilities."""

from __future__ import annotations

import datetime
from zoneinfo import ZoneInfo

from src.utils.ist_clock import (
    IST,
    MarketSession,
    get_market_close_today,
    get_market_session,
    get_market_state_dict,
    get_next_market_open,
    is_market_open,
    is_trading_hours,
    should_scrape,
)


def test_market_session_weekend():
    """Verify that Saturday/Sunday evaluate to WEEKEND session."""
    # Saturday
    dt_sat = datetime.datetime(2026, 6, 20, 10, 0, 0, tzinfo=IST)
    assert get_market_session(dt_sat) == MarketSession.WEEKEND
    assert not is_market_open(dt_sat)
    assert not is_trading_hours(dt_sat)
    assert not should_scrape(dt_sat)

    # Sunday
    dt_sun = datetime.datetime(2026, 6, 21, 12, 0, 0, tzinfo=IST)
    assert get_market_session(dt_sun) == MarketSession.WEEKEND


def test_market_session_holiday():
    """Verify that listed holidays (e.g. Christmas) evaluate to HOLIDAY."""
    # Christmas 2026 is Friday, Dec 25
    dt_holiday = datetime.datetime(2026, 12, 25, 11, 0, 0, tzinfo=IST)
    assert get_market_session(dt_holiday) == MarketSession.HOLIDAY
    assert not is_market_open(dt_holiday)


def test_market_session_weekday_times():
    """Verify market session classification for a normal weekday (Friday)."""
    # 2026-06-19 is a Friday

    # Before pre-open (08:30)
    dt_closed = datetime.datetime(2026, 6, 19, 8, 30, 0, tzinfo=IST)
    assert get_market_session(dt_closed) == MarketSession.CLOSED

    # Pre-open (09:05)
    dt_pre = datetime.datetime(2026, 6, 19, 9, 5, 0, tzinfo=IST)
    assert get_market_session(dt_pre) == MarketSession.PRE_OPEN
    assert is_market_open(dt_pre)
    assert not is_trading_hours(dt_pre)

    # Continuous trading (10:30)
    dt_trading = datetime.datetime(2026, 6, 19, 10, 30, 0, tzinfo=IST)
    assert get_market_session(dt_trading) == MarketSession.CONTINUOUS_TRADING
    assert is_market_open(dt_trading)
    assert is_trading_hours(dt_trading)
    assert should_scrape(dt_trading)

    # Post-close (15:35)
    dt_post = datetime.datetime(2026, 6, 19, 15, 35, 0, tzinfo=IST)
    assert get_market_session(dt_post) == MarketSession.POST_CLOSE
    assert is_market_open(dt_post)
    assert not is_trading_hours(dt_post)

    # After market close (15:50)
    dt_amc = datetime.datetime(2026, 6, 19, 15, 50, 0, tzinfo=IST)
    assert get_market_session(dt_amc) == MarketSession.AFTER_MARKET_CLOSE
    assert not is_market_open(dt_amc)

    # Evening closed (20:00)
    dt_night = datetime.datetime(2026, 6, 19, 20, 0, 0, tzinfo=IST)
    assert get_market_session(dt_night) == MarketSession.CLOSED


def test_next_market_open():
    """Verify computation of next market open datetime."""
    # 2026-06-19 Friday 08:00 -> should open at 09:15 today
    dt_morning = datetime.datetime(2026, 6, 19, 8, 0, 0, tzinfo=IST)
    next_open = get_next_market_open(dt_morning)
    assert next_open.date() == datetime.date(2026, 6, 19)
    assert next_open.time() == datetime.time(9, 15, 0)

    # 2026-06-19 Friday 16:00 -> should open next Monday (2026-06-22)
    dt_evening = datetime.datetime(2026, 6, 19, 16, 0, 0, tzinfo=IST)
    next_open_ev = get_next_market_open(dt_evening)
    assert next_open_ev.date() == datetime.date(2026, 6, 22)
    assert next_open_ev.time() == datetime.time(9, 15, 0)


def test_market_state_dict():
    """Verify structure of market state dict representation."""
    dt = datetime.datetime(2026, 6, 19, 10, 30, 0, tzinfo=IST)
    state = get_market_state_dict(dt)
    assert state["status"] == "OPEN"
    assert state["session"] == "CONTINUOUS_TRADING"
    assert not state["is_holiday"]
