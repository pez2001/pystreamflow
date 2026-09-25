from ..core.node import BaseNode
import asyncio
from collections import deque

class LIFOQueueNode(BaseNode):
    async def init(self):
        self.max_size = int(self.config.get('max_size', 1000))
        # Plain unbounded deque, not deque(maxlen=...) - see the identical
        # note in queue_fifo_node.py: a deque's maxlen is fixed forever at
        # construction, so max_size used to be permanently frozen at
        # whatever it was when init() ran regardless of any later live
        # attribute-wire update. Capacity is enforced manually below,
        # re-reading self.max_size on every item.
        self.queue = deque()

    async def reset(self):
        # See the identical note on StackNode.reset()/FIFOQueueNode.reset():
        # the generic BaseNode.reset() only clears stats/error bookkeeping,
        # so the actual accumulated queue needs its own clear here.
        await super().reset()
        self.queue.clear()

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
            pipes = list(self.inputs.values())
            if not pipes:
                await asyncio.sleep(0.1)
                continue
            got_one = False
            for pipe_in in pipes:
                try:
                    item = await asyncio.wait_for(pipe_in.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                got_one = True
                self.queue.append(item)
                max_size = self.max_size
                if max_size > 0:
                    while len(self.queue) > max_size:
                        self.queue.popleft()
            if got_one or self.queue:
                # LIFO: emit most recent first
                self.emit('out', {'queue': list(self.queue), 'top': self.queue[-1] if self.queue else None})
