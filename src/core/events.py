"""
src/core/events.py
Broadcast real-time events to connected clients (WebSocket / SSE / logs).
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def broadcast(payload: dict[str, Any]) -> None:
    """
    Send *payload* to all subscribed listeners.
    Currently logs to console — replace with your WebSocket/SSE/Redis transport.
    """
    logger.info(f"[event] {payload.get('type','?')} | {payload}")