"""Application configuration via Pydantic Settings — reads from .env file."""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class StockUniverse(str, Enum):
    NIFTY50 = "NIFTY50"
    NIFTY100 = "NIFTY100"
    NIFTY200 = "NIFTY200"
    NIFTY500 = "NIFTY500"
    ALL_NSE = "ALL_NSE"
    ALL_BSE = "ALL_BSE"
    CUSTOM = "CUSTOM"


class ProxyStrategy(str, Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_USED = "least_used"


class Settings(BaseSettings):
    """Master configuration — all values come from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Application ──────────────────────────────────────────────────────
    app_name: str = "bharat-ticker"
    app_env: Literal["development", "staging", "production"] = "development"
    log_level: str = "INFO"
    debug: bool = True

    # ── API Server ───────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = 4
    api_rate_limit_per_minute: int = 120

    # CORS allowed origins (comma-separated env CORS_ORIGINS). Empty + debug →
    # allow all ("*"); empty + production → safe localhost fallback. Set this to
    # the superbrain origin(s) when deploying so cross-origin fetches aren't
    # blocked. Use "*" to allow all (no credentials).
    cors_origins: Annotated[list[str], NoDecode] = Field(default_factory=list)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [o.strip() for o in v.split(",") if o.strip()]
        return v

    # ── API auth ─────────────────────────────────────────────────────────
    # Optional API-key gate. UNSET → auth DISABLED (open) so a deploy can't lock
    # out a running service. Set API_KEY (single) or API_KEYS (comma list) to
    # require an `X-API-Key` header or `?api_key=` on the data API.
    api_key: str = ""
    api_keys: Annotated[list[str], NoDecode] = Field(default_factory=list)

    @field_validator("api_keys", mode="before")
    @classmethod
    def parse_api_keys(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [k.strip() for k in v.split(",") if k.strip()]
        return v

    # ── Redis ────────────────────────────────────────────────────────────
    redis_url: str = "redis://localhost:6379/0"
    redis_max_connections: int = 50
    redis_tick_ttl_seconds: int = 5
    redis_depth_ttl_seconds: int = 2
    redis_meta_ttl_seconds: int = 86400

    # ── TimescaleDB ──────────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://bharat:ticker@localhost:5432/bharat_ticker"
    db_pool_size: int = 20
    db_max_overflow: int = 10

    # ── Scraping Engine ──────────────────────────────────────────────────
    scrape_interval_seconds: float = 1.0
    scrape_concurrency: int = 10
    scrape_timeout_seconds: int = 10
    session_refresh_interval_seconds: int = 90

    # ── Proxy ────────────────────────────────────────────────────────────
    proxy_list: Annotated[list[str], NoDecode] = Field(default_factory=list)
    proxy_rotation_strategy: ProxyStrategy = ProxyStrategy.ROUND_ROBIN
    proxy_health_check_interval: int = 60

    @field_validator("proxy_list", mode="before")
    @classmethod
    def parse_proxy_list(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [p.strip() for p in v.split(",") if p.strip()]
        return v

    # ── Circuit Breaker ──────────────────────────────────────────────────
    cb_failure_threshold: int = 5
    cb_recovery_timeout_seconds: int = 30
    cb_half_open_max_calls: int = 3

    # ── NSE Scraper ──────────────────────────────────────────────────────
    nse_base_url: str = "https://www.nseindia.com"
    nse_enabled: bool = True

    # ── BSE Scraper ──────────────────────────────────────────────────────
    bse_base_url: str = "https://www.bseindia.com"
    bse_enabled: bool = True

    # ── Fyers (Tier-2) ───────────────────────────────────────────────────
    fyers_enabled: bool = False
    fyers_app_id: str = ""
    fyers_secret_key: str = ""
    fyers_redirect_uri: str = ""
    fyers_access_token: str = ""

    # ── Angel One (Tier-2) ───────────────────────────────────────────────
    angel_enabled: bool = False
    angel_api_key: str = ""
    angel_client_id: str = ""
    angel_password: str = ""
    angel_totp_secret: str = ""

    # ── Yahoo Finance (Tier-3) ───────────────────────────────────────────
    yahoo_enabled: bool = True

    # ── EODHD (Tier-3) ──────────────────────────────────────────────────
    eodhd_enabled: bool = False
    eodhd_api_key: str = ""

    # ── Alerts ───────────────────────────────────────────────────────────
    alert_enabled: bool = True
    slack_webhook_url: str = ""
    slack_channel: str = "#market-alerts"
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_email_to: str = ""
    alert_webhook_url: str = ""

    # ── Stock Universe ───────────────────────────────────────────────────
    stock_universe: StockUniverse = StockUniverse.NIFTY50
    custom_symbols: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK"]
    )

    @field_validator("custom_symbols", mode="before")
    @classmethod
    def parse_custom_symbols(cls, v: str | list[str]) -> list[str]:
        if isinstance(v, str):
            return [s.strip().upper() for s in v.split(",") if s.strip()]
        return v

    # ── Derived Paths ────────────────────────────────────────────────────
    @property
    def project_root(self) -> Path:
        return Path(__file__).resolve().parent.parent.parent

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"


# Singleton instance — import this everywhere
settings = Settings()
