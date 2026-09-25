import asyncio
import os
from pathlib import Path

from ..core.node import BaseNode


class DirectoryInputNode(BaseNode):
    """Directory Input Node.

    Watches a directory and emits every entry it finds, split across two
    output ports: real files on ``files``, subdirectories on ``dirs`` -
    so a downstream node can be wired to just one kind without having to
    filter a single mixed stream itself.

    Config:
      path: the directory to scan. Missing/nonexistent/not-a-directory is
        treated as "nothing to emit yet" (recorded in ``_last_error``, not
        raised) rather than a hard failure, since ``path`` is commonly fed
        in later via a live attribute wire rather than set at creation
        time - the same reasoning ``FileInputNode`` already uses for a
        path that doesn't exist yet.
      recursive: bool, default ``False``. ``False`` scans only ``path``'s
        immediate children; ``True`` walks every nested subdirectory too
        (via ``os.walk``), emitting every file/directory found anywhere
        under the tree.
      poll_interval: float seconds between scans, default ``2.0``.

    Both ``path`` and ``recursive`` are re-read from ``self.path``/
    ``self.recursive`` on every scan pass rather than captured once before
    the loop starts, matching the live-attribute-wiring convention
    ``FileInputNode.process()`` established (see its own comment) - so
    wiring a new path in, or toggling ``recursive`` via an attribute edge,
    takes effect on the very next scan instead of requiring a restart.

    Only newly-seen paths are emitted each pass (a per-node in-memory
    "already emitted" set, reset whenever ``path`` changes) rather than
    the full listing every ``poll_interval`` - the same "emit the delta,
    not a repeat of everything so far" shape ``FileInputNode`` follows for
    file content, applied here to directory entries instead of bytes, so
    a long-running watch doesn't flood downstream nodes with the same
    paths over and over.
    """

    async def init(self):
        self.path = self.config.get('path')
        self.recursive = bool(self.config.get('recursive', False))
        self.poll_interval = float(self.config.get('poll_interval', 2.0))

    async def process(self):
        seen_files: set[str] = set()
        seen_dirs: set[str] = set()
        last_path_str: str | None = None
        while self._running:
            path = self.path
            recursive = self.recursive

            if path != last_path_str:
                # Path changed (including going from/to None via a live
                # attribute wire) - start this new target's listing fresh
                # rather than treating everything in it as "already seen"
                # (which would silently emit nothing at all the first time
                # a brand new path is wired in) or, the other way, as if
                # it were still the old path's entries.
                last_path_str = path
                seen_files = set()
                seen_dirs = set()

            if not path:
                await asyncio.sleep(self.poll_interval)
                continue

            base = Path(path)
            if not base.is_dir():
                self._last_error = f'not a directory: {path}'
                await asyncio.sleep(self.poll_interval)
                continue

            try:
                files, dirs = self._scan(base, recursive)
            except OSError as e:
                self._last_error = str(e)
                await asyncio.sleep(self.poll_interval)
                continue

            new_files = [f for f in files if f not in seen_files]
            new_dirs = [d for d in dirs if d not in seen_dirs]
            seen_files.update(new_files)
            seen_dirs.update(new_dirs)

            for f in new_files:
                self.emit('files', f)
            for d in new_dirs:
                self.emit('dirs', d)

            await asyncio.sleep(self.poll_interval)

    def _scan(self, base: Path, recursive: bool) -> tuple[list[str], list[str]]:
        files: list[str] = []
        dirs: list[str] = []
        if recursive:
            for root, dirnames, filenames in os.walk(base):
                for d in dirnames:
                    dirs.append(str(Path(root) / d))
                for f in filenames:
                    files.append(str(Path(root) / f))
        else:
            for entry in base.iterdir():
                if entry.is_dir():
                    dirs.append(str(entry))
                elif entry.is_file():
                    files.append(str(entry))
        return files, dirs
