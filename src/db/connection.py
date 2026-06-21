"""Async PostgreSQL (TimescaleDB) connection pool via asyncpg + SQLAlchemy."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from ..config.settings import settings
from ..utils.logger import get_logger

log = get_logger(__name__)


class DatabaseManager:
    """Manages the async SQLAlchemy engine and session factory."""

    def __init__(self) -> None:
        self._engine: AsyncEngine | None = None
        self._session_factory: sessionmaker | None = None

    async def connect(self) -> None:
        """Initialize the database engine and verify connectivity."""
        if self._engine is not None:
            return

        self._engine = create_async_engine(
            settings.database_url,
            pool_size=settings.db_pool_size,
            max_overflow=settings.db_max_overflow,
            pool_pre_ping=True,
            pool_recycle=3600,
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
