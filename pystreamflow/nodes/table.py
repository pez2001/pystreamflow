from ..core.node import BaseNode
import asyncio
import time

class TableNode(BaseNode):
    async def init(self):
        self.columns = self.config.get('columns', [])
        self.max_rows = int(self.config.get('max_rows', 1000))
        self.emit_interval = float(self.config.get('emit_interval', 5.0))
        self.rows = []
        self.last_emit = 0

    async def reset(self):
        # See the identical note on StackNode.reset(): the generic
        # BaseNode.reset() only clears stats/error bookkeeping, so the
        # actual accumulated table rows need their own clear here.
        await super().reset()
        self.rows = []
        self.last_emit = 0

    async def process(self):
        # Bug fix (found during a codebase-wide audit for this exact
        # shape of problem): port_schema.py deliberately declares this
        # node's inputs as DYNAMIC and documents it as a "true
        # multi-input" node type, in the same category as AndNode/
        # MergeNode - which really do poll every wired input pipe each
        # pass. This node instead only ever read from ONE arbitrary pipe
        # (`next(iter(self.inputs.values()), None)`) and silently never
        # looked at any other input wired under a different port name.
        # Iterating every currently-wired pipe each outer pass (the same
        # pattern AndNode/MergeNode already use) fixes this while leaving
        # single-input use (by far the common case) unchanged.
        while self._running:
            for pipe in list(self.inputs.values()):
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                if isinstance(item, dict):
                    self.rows.append(item)
                else:
                    # treat as row with single column
                    self.rows.append({'value': item})
                if len(self.rows) > self.max_rows:
                    self.rows = self.rows[-self.max_rows:]
            # Emit table snapshot
            now = time.monotonic()
            if now - self.last_emit >= self.emit_interval:
                self.emit('out', {
                    'columns': self.columns,
                    'rows': list(self.rows)
                })
                self.last_emit = now
            await asyncio.sleep(0.1)
