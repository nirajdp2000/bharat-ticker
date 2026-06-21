"""WebSocket streaming endpoint for real-time tick data.

Endpoints:
    WS /api/v1/ws/ticks             — Subscribe to all ticks
    WS /api/v1/ws/ticks/{symbol}    — Subscribe to a specific symbol
"""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from ...cache.redis_client import redis_manager
from ...utils.logger import get_logger

log = get_logger(__name__)
router = APIRouter(tags=["WebSocket"])


class ConnectionManager:
    """Manages active WebSocket connections."""

    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, channel: str = "all") -> None:
        await websocket.accept()
        if channel not in self._connections:
            self._connections[channel] = []
        self._connections[channel].append(websocket)
        log.info("ws_client_connected", channel=channel, total=self._total_connections)

    def disconnect(self, websocket: WebSocket, channel: str = "all") -> None:
        if channel in self._connections:
            self._connections[channel] = [
                ws for ws in self._connections[channel] if ws != websocket
            ]
        log.info("ws_client_disconnected", channel=channel, total=self._total_connections)

    @property
    def _total_connections(self) -> int:
        return sum(len(conns) for conns in self._connections.values())


manager = ConnectionManager()


@router.websocket("/ws/ticks")
async def websocket_all_ticks(websocket: WebSocket):
    """Stream all ticks from all exchanges via Redis Pub/Sub."""
    await manager.connect(websocket, "all")

    try:
        client = redis_manager.client
        pubsub = client.pubsub()
        await pubsub.subscribe("ticks:NSE", "ticks:BSE")

        # Read from pub/sub and forward to WebSocket
        listener_task = asyncio.create_task(_pubsub_listener(pubsub, websocket))

        # Also listen for client messages (ping/pong, unsubscribe)
        try:
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        except WebSocketDisconnect:
            pass
        finally:
            listener_task.cancel()
            await pubsub.unsubscribe("ticks:NSE", "ticks:BSE")
            await pubsub.close()
            manager.disconnect(websocket, "all")

    except Exception as e:
        log.error("ws_error", error=str(e))
        manager.disconnect(websocket, "all")


@router.websocket("/ws/ticks/{symbol}")
async def websocket_symbol_ticks(websocket: WebSocket, symbol: str):
    """Stream ticks for a specific symbol."""
    symbol = symbol.strip().upper()
    channel = f"symbol:{symbol}"
    await manager.connect(websocket, channel)

    try:
        client = redis_manager.client
        pubsub = client.pubsub()
        await pubsub.subscribe("ticks:NSE", "ticks:BSE")

        # Filter for specific symbol
        listener_task = asyncio.create_task(
            _pubsub_listener(pubsub, websocket, symbol_filter=symbol)
        )

        try:
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_text("pong")
        except WebSocketDisconnect:
            pass
        finally:
            listener_task.cancel()
            await pubsub.unsubscribe()
            await pubsub.close()
            manager.disconnect(websocket, channel)

    except Exception as e:
        log.error("ws_symbol_error", symbol=symbol, error=str(e))
        manager.disconnect(websocket, channel)


async def _pubsub_listener(
    pubsub, websocket: WebSocket, symbol_filter: str | None = None
) -> None:
    """Listen to Redis Pub/Sub and forward messages to WebSocket."""
    try:
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue

            data = message["data"]
            if isinstance(data, bytes):
                data = data.decode()

            # Apply symbol filter if set
            if symbol_filter:
                try:
                    parsed = json.loads(data)
                    if parsed.get("symbol") != symbol_filter:
                        continue
                except json.JSONDecodeError:
                    continue

            try:
                await websocket.send_text(data)
            except Exception:
                break
    except asyncio.CancelledError:
        pass
