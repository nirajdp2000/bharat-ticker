"""Async PostgreSQL (TimescaleDB) connection pool via asyncpg + SQLAlchemy."""

from __future__ import annotations

import ssl as _ssl
from typing import Any

from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from ..config.settings import settings
from ..utils.logger import get_logger

log = get_logger(__name__)

# libpq/psycopg query params that asyncpg does NOT understand — must be stripped
# from the URL and translated into connect_args (asyncpg wants ssl=SSLContext).
_LIBPQ_SSL_KEYS = ("sslmode", "channel_binding", "sslrootcert", "sslcert", "sslkey")


def _normalize_db_url(raw: str) -> tuple[Any, dict[str, Any]]:
    """Coerce any Postgres URL into an asyncpg URL + connect_args.

    Accepts plain ``postgresql://`` (e.g. straight from Northflank/Supabase with
    ``?sslmode=require``) and returns a ``postgresql+asyncpg`` URL with the libpq
    ssl params removed, plus an ssl SSLContext when TLS is requested. This lets a
    deployer paste the provider's URI verbatim and have it connect over TLS.
    """
    url = make_url(raw)
    # Force the asyncpg driver regardless of how the URL was written.
    if url.drivername in ("postgresql", "postgres", "postgresql+psycopg2", "postgresql+psycopg"):
        url = url.set(drivername="postgresql+asyncpg")

    query = dict(url.query)
    sslmode = (query.get("sslmode") or query.get("ssl") or "").lower()
    for k in (*_LIBPQ_SSL_KEYS, "ssl"):
        query.pop(k, None)
    url = url.set(query=query)

    connect_args: dict[str, Any] = {}
    want_tls = sslmode in ("require", "prefer", "allow", "verify-ca", "verify-full", "true", "1")
    if want_tls:
        ctx = _ssl.create_default_context()
        # require/prefer = encrypt without CA verification (managed addons use a
        # self-signed/internal CA); verify-* keeps full verification.
        if sslmode in ("verify-ca", "verify-full"):
            pass
        else:
            ctx.check_hostname = False
            ctx.verify_mode = _ssl.CERT_NONE
        connect_args["ssl"] = ctx
    return url, connect_args


class DatabaseManager:
    """Manages the async SQLAlchemy engine and session factory."""

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._session_factory: sessionmaker | None = None

    async def connect(self) -> None:
        """Initialize the database engine and verify connectivity."""
        if self._engine is not None:
            return

        url, connect_args = _normalize_db_url(settings.database_url)
        self._engine = create_async_engine(
            url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args=connect_args,
            echo=settings.debug and not settings.is_production,
        )
        self._session_factory = sessionmaker(
            bind=self._engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

        # Verify connectivity — if it fails, tear down so is_connected is honest
        # (otherwise endpoints would block on a dead DB before falling back).
        try:
            async with self._engine.begin() as conn:
                await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
                log.info("database_connected", url=settings.database_url.split("@")[-1])
        except Exception as e:
            log.error("database_connection_failed", error=str(e))
            try:
                await self._engine.dispose()
            except Exception:
                pass
            self._engine = None
            self._session_factory = None
            raise

        # Auto-apply the schema (idempotent: CREATE … IF NOT EXISTS + guarded
        # Timescale block). Runs from INSIDE the deploy where the addon host
        # resolves, so no manual psql is needed. Best-effort — a schema hiccup
        # must not knock the DB offline.
        await self._apply_schema()

    async def _apply_schema(self) -> None:
        from pathlib import Path

        schema = Path(__file__).resolve().parent / "schema.sql"
        if not schema.exists():
            return
        try:
            sql = schema.read_text(encoding="utf-8")
            # asyncpg rejects multi-statement strings through the prepared-statement
            # path (exec_driver_sql → "cannot insert multiple commands into a
            # prepared statement"), which silently dropped the WHOLE schema. Reach
            # the raw asyncpg connection and use its simple-query protocol, which
            # runs the full file in one shot (incl. the guarded DO $$ block).
            async with self._engine.connect() as conn:
                raw = await conn.get_raw_connection()
                await raw.driver_connection.execute(sql)
            log.info("database_schema_applied")
        except Exception as e:  # noqa: BLE001
            log.warning("database_schema_apply_failed", error=str(e))

    async def disconnect(self) -> None:
        """Dispose of the engine and close all connections."""
        if self._engine:
            await self._engine.dispose()
            self._engine = None
            self._session_factory = None
            log.info("database_disconnected")

    @property
    def is_connected(self) -> bool:
        """True if the engine is initialized (DB reachable)."""
        return self._session_factory is not None

    def get_session(self) -> AsyncSession:
        """Get a new async session. Use as async context manager."""
        if self._session_factory is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._session_factory()

    @property
    def engine(self) -> AsyncEngine:
        if self._engine is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._engine

    async def health_check(self) -> bool:
        """Check database connectivity."""
        try:
            async with self._engine.begin() as conn:
                await conn.execute(__import__("sqlalchemy").text("SELECT 1"))
            return True
        except Exception:
            return False


# Singleton
db_manager = DatabaseManager()
