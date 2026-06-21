"""In-process async Redis-compatible fallback store.

When a real Redis server is not reachable, the system degrades to this
zero-dependency in-memory implementation so the API + scraper still boot
and serve data on a single machine.  It implements ONLY the subset of the
``redis.asyncio`` interface used by this codebase:

    ping, get, set, incr, expire, hset, hgetall,
    xadd, xgroup_create, xreadgroup, xack,
    publish, pubsub(), pipeline()

It is NOT a drop-in for production multi-process deployments (no cross-process
sharing, no persistence) — it exists purely so a developer with no Redis/Docker
can run the whole stack in one process.  In production, point ``REDIS_URL`` at a
real server and this is never used.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from typing import Any

from ..utils.logger import get_logger

log = get_logger(__name__)


class _MemoryPubSub:
    """Minimal async Pub/Sub mirroring redis.asyncio pubsub()."""

    def __init__(self, store: "MemoryStore") -> None:
        self._store = store
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._channels: set[str] = set()

    async def subscribe(self, *channels: str) -> None:
        for ch in channels:
            self._channels.add(ch)
            self._store._subscribers[ch].add(self._queue)

    async def unsubscribe(self, *channels: str) -> None:
        targets = channels or tuple(self._channels)
        for ch in targets:
            self._channels.discard(ch)
            self._store._subscribers[ch].discard(self._queue)

    async def listen(self):
        while True:
            msg = await self._queue.get()
            yield msg

    async def close(self) -> None:
        await self.unsubscribe()

    aclose = close


class _MemoryPipeline:
    """Buffers operations and executes them sequentially (no MULTI/EXEC)."""

    def __init__(self, store: "MemoryStore") -> None:
        self._store = store
        self._ops: list[tuple[str, tuple, dict]] = []

    def __getattr__(self, name: str):
        # Buffer any store method call: pipe.hset(...), pipe.publish(...), etc.
        def _record(*args: Any, **kwargs: Any) -> "_MemoryPipeline":
            self._ops.append((name, args, kwargs))
            return self
        return _record

    async def execute(self) -> list[Any]:
        results = []
        for name, args, kwargs in self._ops:
            fn = getattr(self._store, name)
            results.append(await fn(*args, **kwargs))
        self._ops.clear()
        return results


class MemoryStore:
    """Async in-memory key/value + hash + stream + pub/sub store."""

    def __init__(self) -> None:
        self._kv: dict[str, Any] = {}
        self._hashes: dict[str, dict[str, str]] = {}
        self._expiry: dict[str, float] = {}
        self._streams: dict[str, deque[tuple[str, dict]]] = defaultdict(lambda: deque(maxlen=100000))
        self._stream_seq: dict[str, int] = defaultdict(int)
        self._subscribers: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self._groups: dict[str, dict[str, int]] = defaultdict(dict)  # stream -> {group: last_read_idx}

    # ── Expiry helper ────────────────────────────────────────────────────
    def _expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        if exp is not None and time.time() >= exp:
            self._kv.pop(key, None)
            self._hashes.pop(key, None)
            self._expiry.pop(key, None)
            return True
        return False

    # ── Connection ───────────────────────────────────────────────────────
    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None

    # ── String ops ───────────────────────────────────────────────────────
    async def get(self, key: str) -> Any:
        if self._expired(key):
            return None
        return self._kv.get(key)

    async def set(self, key: str, value: Any, ex: int | None = None) -> bool:
        self._kv[key] = value
        if ex:
            self._expiry[key] = time.time() + ex
        else:
            self._expiry.pop(key, None)
        return True

    async def incr(self, key: str) -> int:
        if self._expired(key):
            self._kv[key] = 0
        current = int(self._kv.get(key, 0))
        current += 1
        self._kv[key] = current
        return current

    async def expire(self, key: str, ttl: int) -> bool:
        if key in self._kv or key in self._hashes:
            self._expiry[key] = time.time() + ttl
            return True
        return False

    # ── Hash ops ─────────────────────────────────────────────────────────
    async def hset(self, key: str, *, mapping: dict[str, Any] | None = None, **kwargs: Any) -> int:
        h = self._hashes.setdefault(key, {})
        items = dict(mapping or {})
        items.update(kwargs)
        for k, v in items.items():
            h[str(k)] = str(v)
        return len(items)

    async def hgetall(self, key: str) -> dict[str, str]:
        if self._expired(key):
            return {}
        return dict(self._hashes.get(key, {}))

    # ── Stream ops ───────────────────────────────────────────────────────
    async def xadd(self, stream: str, fields: dict[str, Any], maxlen: int | None = None, **_: Any) -> str:
        self._stream_seq[stream] += 1
        msg_id = f"{int(time.time() * 1000)}-{self._stream_seq[stream]}"
        self._streams[stream].append((msg_id, {str(k): str(v) for k, v in fields.items()}))
        return msg_id

    async def xgroup_create(self, stream: str, group: str, id: str = "0", mkstream: bool = False) -> bool:
        _ = self._streams[stream]  # ensure stream exists
        self._groups[stream][group] = 0
        return True

    async def xreadgroup(self, groupname: str, consumername: str, streams: dict[str, str],
                         count: int = 100, block: int | None = None) -> list:
        # Simplified: return any unread messages; if none, honor block delay once.
        out = []
        for stream in streams:
            buf = list(self._streams.get(stream, ()))
            last = self._groups[stream].get(groupname, 0)
            new = buf[last:last + count]
            if new:
                self._groups[stream][groupname] = last + len(new)
                out.append((stream, new))
        if not out and block:
            await asyncio.sleep(min(block, 1000) / 1000)
        return out

    async def xack(self, stream: str, group: str, *msg_ids: str) -> int:
        return len(msg_ids)

    # ── Pub/Sub ──────────────────────────────────────────────────────────
    async def publish(self, channel: str, message: Any) -> int:
        subs = self._subscribers.get(channel)
        if not subs:
            return 0
        payload = {"type": "message", "channel": channel, "data": message}
        for q in list(subs):
            q.put_nowait(payload)
        return len(subs)

    def pubsub(self) -> _MemoryPubSub:
        return _MemoryPubSub(self)

    # ── Pipeline ─────────────────────────────────────────────────────────
    def pipeline(self, transaction: bool = False) -> _MemoryPipeline:
        return _MemoryPipeline(self)
