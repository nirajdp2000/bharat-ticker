"""Unit tests for the FastAPI API endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.utils.ist_clock import MarketSession


@pytest.fixture
def client() -> TestClient:
    """Create a FastAPI test client."""
    return TestClient(app)


def test_root_endpoint(client):
    """Verify that the API metadata endpoint returns system metadata.

    Root ``/`` now serves the HTML dashboard; JSON metadata lives at ``/api``.
    """
    response = client.get("/api")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Bharat Ticker"
    assert "market_state" in data


def test_ping_endpoint(client):
    """Verify that the ping endpoint responds with pong."""
    response = client.get("/api/v1/ping")
    assert response.status_code == 200
    assert response.json()["pong"] is True


def test_health_check_endpoint(client):
    """Verify health check shows correct component statuses."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in ("healthy", "degraded")
    assert "redis" in data["components"]
    assert "timescaledb" in data["components"]


def test_get_quote_realtime(client, mock_redis, monkeypatch):
    """Verify retrieving a quote from Redis cache during market hours."""
    # Mock market hours to be open
    monkeypatch.setattr("src.api.endpoints.quotes.is_market_open", lambda: True)

    # Seed the mock Redis cache with a tick
    tick_key = "stock:NSE:RELIANCE:latest"
    mock_redis.data[tick_key] = {
        "symbol": "RELIANCE",
        "exchange": "NSE",
        "isin": "INE002A01018",
        "series": "EQ",
        "ltp": "2945.50",
        "open": "2930.00",
        "high": "2958.75",
        "low": "2925.10",
        "close": "2935.25",
        "change": "10.25",
        "pct_change": "0.35",
        "volume": "4523891",
        "value": "13311567432.50",
        "vwap": "2941.87",
        "upper_circuit": "3228.75",
        "lower_circuit": "2641.75",
        "week_52_high": "3217.90",
        "week_52_low": "2220.30",
        "timestamp": "2026-06-19T10:30:00+05:30",
        "source": "nse_scraper",
        "source_latency_ms": "45.0",
        "cached_at": "1781912400.0",  # Mock timestamp
    }

    response = client.get("/api/v1/quote/RELIANCE")
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["status"] == "success"
    assert res_data["data"]["info"]["symbol"] == "RELIANCE"
    # Prices are serialized as Decimal strings to preserve precision.
    assert float(res_data["data"]["price"]["ltp"]) == 2945.5
    assert res_data["meta"]["source"] == "nse_scraper"


def test_get_quote_eod_fallback(client, mock_redis, mock_db, monkeypatch):
    """Verify retrieving a quote from TimescaleDB when closed and not in cache."""
    # Mock market hours to be closed
    monkeypatch.setattr("src.api.endpoints.quotes.is_market_open", lambda: False)

    # Ensure Redis cache has no entry for RELIANCE
    # Mock DB EOD query response
    mock_db.execute = AsyncMock()
    # Mocking TickerQueries.get_latest_eod
    # TickerQueries is instantiated with the session, and get_latest_eod is called
    mock_get_latest_eod = AsyncMock(return_value={
        "open": 2930.00,
        "high": 2958.75,
        "low": 2925.10,
        "close": 2935.25,
        "volume": 4523891,
        "value": 13311567432.50,
        "vwap": 2941.87,
        "delivery_qty": 2000000,
        "delivery_pct": 44.21,
    })
    monkeypatch.setattr("src.db.queries.TickerQueries.get_latest_eod", mock_get_latest_eod)

    response = client.get("/api/v1/quote/RELIANCE")
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["status"] == "success"
    assert res_data["data"]["info"]["symbol"] == "RELIANCE"
    # Prices are serialized as Decimal strings to preserve precision.
    assert float(res_data["data"]["price"]["close"]) == 2935.25
    assert res_data["meta"]["source"] == "timescaledb"
