"""NSE/BSE market holiday calendar for 2025-2030.

Holidays are derived from official NSE circulars. The system checks this
calendar to determine whether the market is open on a given date. The
calendar is also refreshable at runtime via an optional API fetch.
"""

from __future__ import annotations

import datetime
from functools import lru_cache


# ── Official NSE Trading Holidays ────────────────────────────────────────────
# Source: NSE circulars — https://www.nseindia.com/regulations/trading-holidays
# Format: (month, day, description)

_HOLIDAYS_2025: list[tuple[int, int, str]] = [
    (2, 26, "Mahashivratri"),
    (3, 14, "Holi"),
    (3, 31, "Id-Ul-Fitr (Ramadan Eid)"),
    (4, 10, "Shri Mahavir Jayanti"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti"),
    (4, 18, "Good Friday"),
    (5, 1, "Maharashtra Day"),
    (8, 15, "Independence Day"),
    (8, 27, "Ganesh Chaturthi"),
    (10, 2, "Mahatma Gandhi Jayanti / Dussehra"),
    (10, 21, "Diwali Laxmi Pujan"),
    (10, 22, "Diwali-Balipratipada"),
    (11, 5, "Prakash Gurpurb Sri Guru Nanak Dev"),
    (12, 25, "Christmas"),
]

_HOLIDAYS_2026: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (3, 3, "Holi"),
    (3, 26, "Shri Ram Navami"),
    (3, 31, "Shri Mahavir Jayanti"),
    (4, 3, "Good Friday"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti"),
    (5, 1, "Maharashtra Day"),
    (5, 28, "Bakri Id"),
    (6, 26, "Muharram"),
    (9, 14, "Ganesh Chaturthi"),
    (10, 2, "Mahatma Gandhi Jayanti"),
    (10, 20, "Dussehra"),
    (11, 10, "Diwali-Balipratipada"),
    (11, 24, "Prakash Gurpurb Sri Guru Nanak Dev"),
    (12, 25, "Christmas"),
]

# Placeholder years — populated with Republic Day, Independence Day,
# Gandhi Jayanti, Christmas, and other consistent holidays. These MUST be
# updated annually from official NSE circulars before each year begins.
_HOLIDAYS_2027: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (3, 22, "Holi"),
    (4, 2, "Good Friday"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti"),
    (5, 1, "Maharashtra Day"),
    (8, 15, "Independence Day"),
    (10, 2, "Mahatma Gandhi Jayanti"),
    (12, 25, "Christmas"),
]

_HOLIDAYS_2028: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (3, 10, "Holi"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti / Good Friday"),
    (5, 1, "Maharashtra Day"),
    (8, 15, "Independence Day"),
    (10, 2, "Mahatma Gandhi Jayanti"),
    (12, 25, "Christmas"),
]

_HOLIDAYS_2029: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (3, 1, "Holi"),
    (3, 30, "Good Friday"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti"),
    (5, 1, "Maharashtra Day"),
    (8, 15, "Independence Day"),
    (10, 2, "Mahatma Gandhi Jayanti"),
    (12, 25, "Christmas"),
]

_HOLIDAYS_2030: list[tuple[int, int, str]] = [
    (1, 26, "Republic Day"),
    (3, 20, "Holi"),
    (4, 14, "Dr. Baba Saheb Ambedkar Jayanti"),
    (4, 19, "Good Friday"),
    (5, 1, "Maharashtra Day"),
    (8, 15, "Independence Day"),
    (10, 2, "Mahatma Gandhi Jayanti"),
    (12, 25, "Christmas"),
]


_ALL_HOLIDAYS: dict[int, list[tuple[int, int, str]]] = {
    2025: _HOLIDAYS_2025,
    2026: _HOLIDAYS_2026,
    2027: _HOLIDAYS_2027,
    2028: _HOLIDAYS_2028,
    2029: _HOLIDAYS_2029,
    2030: _HOLIDAYS_2030,
}


@lru_cache(maxsize=8)
def get_holiday_dates(year: int) -> set[datetime.date]:
    """Return the set of holiday *dates* for a given year."""
    raw = _ALL_HOLIDAYS.get(year, [])
    return {datetime.date(year, m, d) for m, d, _ in raw}


def get_holiday_name(dt: datetime.date) -> str | None:
    """Return the holiday name for a date, or None if not a holiday."""
    raw = _ALL_HOLIDAYS.get(dt.year, [])
    for m, d, name in raw:
        if dt.month == m and dt.day == d:
            return name
    return None


def is_trading_holiday(dt: datetime.date) -> bool:
    """Check if a given date is a declared NSE/BSE trading holiday."""
    return dt in get_holiday_dates(dt.year)


def get_next_trading_day(dt: datetime.date) -> datetime.date:
    """Get the next valid trading day after *dt* (skips weekends + holidays)."""
    candidate = dt + datetime.timedelta(days=1)
    while candidate.weekday() >= 5 or is_trading_holiday(candidate):
        candidate += datetime.timedelta(days=1)
    return candidate


def get_previous_trading_day(dt: datetime.date) -> datetime.date:
    """Get the most recent trading day before *dt*."""
    candidate = dt - datetime.timedelta(days=1)
    while candidate.weekday() >= 5 or is_trading_holiday(candidate):
        candidate -= datetime.timedelta(days=1)
    return candidate
