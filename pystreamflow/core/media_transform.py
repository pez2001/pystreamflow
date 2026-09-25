"""Shared ``process()`` loop for nodes that transform one ``MediaItem``
into another (media plan phase 3) - the media counterpart of
``core/transform_node.py``'s ``SingleInputTransformNode``.

The differences from that class are what media work needs:

- ``transform_media()`` runs in a worker thread (``asyncio.to_thread``),
  so decoding/encoding/resizing a large image never blocks the event loop
  and every other node with it.
- Only ``MediaItem``s of the kinds in ``ACCEPT_KINDS`` are transformed;
  anything else (a text item, an audio clip reaching an image node) is
  passed through unchanged, so a mixed stream keeps flowing.
- A failing transform (a corrupt file, a blob that expired) doesn't crash
  the node: the original item is passed through, like the numeric/text
  families do, but the failure is counted in the node's
  ``error_count``/``last_error`` so it is visible in the editor.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from .media import MediaItem
from .node import BaseNode

logger = logging.getLogger("pystreamflow.media_transform")


class MediaTransformNode(BaseNode):
    ACCEPT_KINDS: tuple[str, ...] = ("image", "video_frame")

    async def configure(self) -> None:
        """One-time config parsing, called from ``init()``. Raise
        ``ValueError`` for invalid config - the node then shows as
        ``error`` instead of starting."""
        return None

    async def init(self):
        await self.configure()

    def accepts(self, item: Any) -> bool:
        return isinstance(item, MediaItem) and item.kind in self.ACCEPT_KINDS

    def transform_media(self, item: MediaItem) -> Any:
        """Blocking per-item work (runs in a worker thread). Return the
        item to emit on ``out``."""
        raise NotImplementedError(f"{type(self).__name__} must implement transform_media(item)")

    async def handle(self, item: Any) -> Any:
        """Transform one item the way ``process()`` does; returns what was
        emitted. Also handy for driving a node directly in tests."""
        if not self.accepts(item):
            self.emit('out', item)
            return item
        try:
            result = await asyncio.to_thread(self.transform_media, item)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            logger.warning("node %s (%s): %s - passing the item through unchanged",
                           self.id, type(self).__name__, self._last_error)
            result = item
        self.emit('out', result)
        return result

    async def process(self):
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self.handle(item)
