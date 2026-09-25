import asyncio
import json
import sys
from pathlib import Path

from ..core.node import BaseNode


class JSONInputNode(BaseNode):
    """JSON Input Node.

    Reads newline-delimited JSON from a real stream and emits each
    successfully parsed line's value directly on ``out`` - "normalizing"
    the raw JSON text into a real Python object, per the node's own
    description in the editor/docs.

    Bug found and fixed while auditing the codebase for exactly this
    shape of problem (see ``FileOutputNode``'s "never actually writes a
    file" bug from an earlier round): despite reading ``config['source']``
    into ``self.source`` in ``init()``, the old ``process()`` never
    referenced it again anywhere - it unconditionally emitted one fixed
    synthetic sample (``{"type": "json", "data": {"ts": time.time()}}``)
    once a second forever, regardless of what ``source`` said or any real
    external input. A user configuring ``source`` from the editor (had
    there been a widget for it - there wasn't one) would have seen zero
    effect from any value they chose.

    Config:
      source: ``'stdin'`` (default) to read from this process's real
        standard input, or a filesystem path to tail (same "keep reading
        newly-appended lines" semantics as ``FileInputNode``) - useful
        for feeding this node from another process, a fifo, or a
        continuously-appended log/export file.
    """

    async def init(self):
        self.source = self.config.get('source', 'stdin')
        self._last_size = 0

    async def process(self):
        if self.source == 'stdin':
            await self._process_stdin()
        else:
            await self._process_file(self.source)

    async def _process_stdin(self):
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        try:
            await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        except (ValueError, OSError) as e:
            # sys.stdin isn't a real, connectable pipe in this process
            # (already closed, redirected from something connect_read_pipe
            # can't wrap, or - commonly in a test/daemon context - not a
            # pipe at all). Report it clearly rather than silently idling
            # forever or crashing the node's task.
            self._last_error = f'could not attach to stdin: {e}'
            return
        while self._running:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not line:
                # EOF - stdin was closed. Nothing more will ever arrive.
                break
            self._emit_line(line.decode('utf-8', errors='replace'))

    async def _process_file(self, source: str):
        path = Path(source)
        last_size = 0
        while self._running:
            if not path.exists():
                await asyncio.sleep(0.5)
                continue
            try:
                size = path.stat().st_size
            except OSError as e:
                self._last_error = str(e)
                await asyncio.sleep(0.5)
                continue
            if size <= last_size:
                await asyncio.sleep(0.5)
                continue
            try:
                # Path.read_bytes() (like FileInputNode already uses for
                # its own tailing loop) rather than a plain open()/seek(),
                # so this stays consistent with that existing convention
                # and avoids a blocking `open()` call directly inside this
                # async function.
                new_bytes = path.read_bytes()[last_size:]
            except OSError as e:
                self._last_error = str(e)
                await asyncio.sleep(0.5)
                continue
            new_text = new_bytes.decode('utf-8', errors='replace')
            last_size = size
            for line in new_text.splitlines():
                self._emit_line(line)

    def _emit_line(self, line: str):
        line = line.strip()
        if not line:
            return
        try:
            value = json.loads(line)
        except (json.JSONDecodeError, ValueError) as e:
            self._last_error = str(e)
            self.emit('out', {'error': str(e), 'raw': line})
            return
        self.emit('out', value)
