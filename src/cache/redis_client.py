"""Redis connection pool manager with async support."""

from __future__ import annotations

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis

from ..config.settings import settings
from ..utils.logger import get_logger
from .memory_store import MemoryStore

log = get_logger(__name__)


class RedisManager:
    """Manages the async Redis connection pool.

    If a real Redis server is unreachable, transparently falls back to an
    in-process :class:`MemoryStore` so the system still boots on a single
    machine with no external infrastructure.  ``is_real`` reports which
    backend is active.

    Usage::

        redis_mgr = RedisManager()
        await redis_mgr.connect()
        client = redis_mgr.client
        await client.set("key", "value")
        await redis_mgr.disconnect()
    """

    def __init__(self) -> None:
        self._pool: ConnectionPool | None = None
        self._client: Redis | MemoryStore | None = None
        self._is_real: bool = False

    async def connect(self) -> None:
        """Initialize the connection pool, falling back to in-memory on failure."""
        if self._client is not None:
            return

        try:
            self._pool = ConnectionPool.from_url(
                settings.redis_url,
                max_connections=settings.redis_max_connections,
                decode_responses=True,
                socket_connect_timeout=2,
                socket_timeout=5,
                retry_on_timeout=True,
            )
            client = Redis(connection_pool=self._pool)
            info = await client.ping()
            self._client = client
            self._is_real = True
            log.info("redis_connected", url=settings.redis_url, ping=info)
        except Exception as e:
            # Degrade gracefully to an in-process store.
            if self._pool is not None:
                try:
                    await self._pool.disconnect()
                except Exception:
                    pass
                self._pool = None
            self._client = MemoryStore()
            self._is_real = False
            log.warning(
                "redis_unavailable_using_memory_store",
                error=str(e),
                note="In-process fallback active — single-process only. Set a reachable REDIS_URL for production.",
            )

    @property
    def is_real(self) -> bool:
        """True if backed by a real Redis server, False if in-memory fallback."""
        return self._is_real

    async def disconnect(self) -> None:
        """Close all connections in the pool."""
        if self._client:
            await self._client.aclose()
            self._client = None
        if self._pool:
            await self._pool.disconnect()
            self._pool = None
        log.info("redis_disconnected")

    @property
    def client(self) -> Redis:
        """Get the async Redis client. Raises if not connected."""
        if self._client is None:
            raise RuntimeError("Redis not connected. Call connect() first.")
        return self._client

    async def health_check(self) -> bool:
        """Check Redis connectivity."""
        try:
            return await self.client.ping()
        except Exception:
            return False


# Singleton
redis_manager = RedisManager()
