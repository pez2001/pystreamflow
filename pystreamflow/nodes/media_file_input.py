import asyncio
import os
from pathlib import Path

from ..core.media import MediaItem, guess_mime
from ..core.node import BaseNode


def load_media_file(path: str, extra_meta: dict | None = None) -> MediaItem:
    """Read a whole file into a ``MediaItem`` (blocking - call it through
    ``asyncio.to_thread``). MIME type from magic bytes, then extension."""
    p = Path(path)
    data = p.read_bytes()
    st = p.stat()
    meta = {"source_path": str(p), "filename": p.name, "mtime": st.st_mtime}
    if extra_meta:
        meta.update(extra_meta)
    return MediaItem.from_bytes(data, mime=guess_mime(data, p.name), meta=meta)


class MediaFileInputNode(BaseNode):
    """Media File Input Node (media plan phase 2).

    Reads one image/audio/video file *as a whole* and emits it as a single
    ``MediaItem`` on ``out`` - unlike ``FileInputNode``, which tails a file
    and emits whatever bytes were appended, which for a binary file means
    arbitrary fragments instead of one decodable image. The read and the
    blob-store write run in a worker thread, so a large file never blocks
    the event loop.

    Config:
      path: the file to read. May be fed in later via an attribute wire; a
        missing file is "nothing to emit yet" (recorded in ``last_error``),
        not a failure.
      emit_on: ``change`` (default) emits once on start and again whenever
        the file's size or mtime changes, or ``path`` is rewired; ``start``
        emits once per path and then only on manual emit.
      poll_interval: seconds between checks for ``change`` (default 1.0).
    """

    # One whole file per item - never drop one because a consumer is slow.
    MEDIA_OUTPUT_PORTS = {"out": "block"}

    async def init(self):
        self.path = self.config.get('path')
        self.emit_on = str(self.config.get('emit_on', 'change')).lower()
        if self.emit_on not in ('change', 'start'):
            raise ValueError(f"MediaFileInputNode: emit_on must be 'change' or 'start', not {self.emit_on!r}")
        self.poll_interval = float(self.config.get('poll_interval', 1.0))

    async def process(self):
        last_path = None
        last_sig = None
        while self._running:
            path = self.path
            if path != last_path:
                last_path = path
                last_sig = None
            sig = None
            if path:
                try:
                    st = await asyncio.to_thread(os.stat, path)
                    sig = (st.st_size, st.st_mtime_ns)
                except OSError as e:
                    self._last_error = f'cannot read {path}: {e.strerror or e}'
            should_emit = sig is not None and (
                last_sig is None if self.emit_on == 'start' else sig != last_sig
            )
            if should_emit:
                try:
                    item = await asyncio.to_thread(load_media_file, path)
                except OSError as e:
                    self._last_error = f'cannot read {path}: {e.strerror or e}'
                else:
                    last_sig = sig
                    self._last_error = None
                    await self.emit_wait('out', item)
            await asyncio.sleep(self.poll_interval)
