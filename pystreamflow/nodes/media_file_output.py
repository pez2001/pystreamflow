import asyncio
import os
import time
from pathlib import Path

from ..core.media import MediaItem, extension_for_mime
from ..core.node import BaseNode
from ..core.paths import ensure_parent_dir, files_dir


class _Fields(dict):
    """format_map() source that leaves unknown placeholders in place
    instead of raising KeyError."""

    def __missing__(self, key):
        return "{" + key + "}"


class MediaFileOutputNode(BaseNode):
    """Media File Output Node (media plan phase 2).

    Writes every incoming item to its *own* file - unlike
    ``FileOutputNode``, which appends everything to one file. Meant for
    ``MediaItem``s (the extension follows the item's MIME type), but also
    accepts raw ``bytes`` (type sniffed) and anything else (written as
    UTF-8 text). The write runs in a worker thread.

    After writing, emits ``{'path', 'mime', 'size'}`` on ``out`` so the
    written file can be wired onward (e.g. into a path attribute), and
    records the item in the live view, where it gets a preview.

    Config:
      path_pattern: target path, a ``str.format`` pattern with the fields
        ``{node}``, ``{index}`` (per-node counter, e.g. ``{index:05d}``),
        ``{ext}``, ``{kind}``, ``{stem}`` (the source file's name without
        extension, or ``item``) and ``{timestamp}`` (Unix seconds).
        Default ``<files_dir>/media/{node}_{index:05d}.{ext}``.
      overwrite: if false (default), an existing file is never replaced -
        the index is advanced until the name is free (for patterns without
        ``{index}``, ``_1``, ``_2``, ... is appended).
    """

    async def init(self):
        default = os.path.join(files_dir(), 'media', '{node}_{index:05d}.{ext}')
        self.path_pattern = self.config.get('path_pattern') or default
        self.overwrite = bool(self.config.get('overwrite', False))
        self._index = 0

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
            await self.write(item)

    @staticmethod
    def _as_media(item) -> MediaItem:
        if isinstance(item, MediaItem):
            return item
        if isinstance(item, (bytes, bytearray, memoryview)):
            return MediaItem.from_bytes(bytes(item))
        return MediaItem.from_bytes(str(item).encode('utf-8'), mime='text/plain')

    def _target(self, media: MediaItem) -> str:
        stem = Path(str(media.meta.get('filename') or media.meta.get('source_path') or 'item')).stem or 'item'
        base = {
            'node': self.id, 'ext': extension_for_mime(media.mime), 'kind': media.kind,
            'stem': stem, 'timestamp': int(time.time()),
        }
        uses_index = '{index' in self.path_pattern
        suffix = 0
        while True:
            path = self.path_pattern.format_map(_Fields(base, index=self._index))
            if suffix and not uses_index:
                root, ext = os.path.splitext(path)
                path = f"{root}_{suffix}{ext}"
            if self.overwrite or not os.path.exists(path):
                self._index += 1
                return path
            if uses_index:
                self._index += 1
            else:
                suffix += 1

    def _write_blocking(self, item) -> dict:
        media = self._as_media(item)
        data = media.get_bytes()
        path = self._target(media)
        ensure_parent_dir(path)
        with open(path, 'wb') as f:
            f.write(data)
        return {'path': path, 'mime': media.mime, 'size': len(data), 'media': media}

    async def write(self, item) -> dict | None:
        try:
            result = await asyncio.to_thread(self._write_blocking, item)
        except (OSError, KeyError, ValueError) as e:
            # KeyError: the item's blob expired before it could be written.
            self._error_count += 1
            self._last_error = f'write failed: {e}'
            self.record_output({'error': self._last_error, 'item': item})
            return None
        media = result.pop('media')
        self.record_output({'written': result['path'], 'bytes_written': result['size'], 'item': media})
        self.emit('out', result)
        return result
