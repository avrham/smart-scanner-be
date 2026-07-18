"""
Simple in-process event bus for streaming scan progress over WebSockets
Buffers the latest event per scan so late subscribers immediately catch up.
"""

import asyncio
from typing import Dict, Set, Any, Optional


class ScanEventBus:
    def __init__(self):
        self._subscribers: Dict[str, Set[asyncio.Queue]] = {}
        self._latest: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def publish(self, scan_id: str, event: Dict[str, Any]) -> None:
        async with self._lock:
            queues = list(self._subscribers.get(scan_id, set()))
            # Save last event so new subscribers get immediate state
            self._latest[scan_id] = event
        for q in queues:
            try:
                await q.put(event)
            except Exception:
                # Ignore broken subscribers
                pass

    async def subscribe(self, scan_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        async with self._lock:
            self._subscribers.setdefault(scan_id, set()).add(q)
        return q

    async def unsubscribe(self, scan_id: str, q: asyncio.Queue) -> None:
        async with self._lock:
            subs = self._subscribers.get(scan_id)
            if subs and q in subs:
                subs.remove(q)
            if subs and len(subs) == 0:
                self._subscribers.pop(scan_id, None)

    async def latest(self, scan_id: str) -> Optional[Dict[str, Any]]:
        async with self._lock:
            return self._latest.get(scan_id)


event_bus = ScanEventBus()


