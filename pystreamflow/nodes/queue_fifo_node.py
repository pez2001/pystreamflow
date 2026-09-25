from ..core.node import BaseNode
import asyncio
from collections import deque

class FIFOQueueNode(BaseNode):
    async def init(self):
        self.max_size = int(self.config.get('max_size', 1000))
        self.drop_policy = self.config.get('drop_policy', 'drop')  # drop, block
        # A plain, unbounded deque - not deque(maxlen=self.max_size). A
        # deque's own maxlen is fixed at construction and can never be
        # changed afterwards, so capacity used to be permanently frozen at
        # whatever max_size was when init() ran: a live attribute-wire
        # update to max_size updated self.max_size/self.config correctly
        # but had zero effect on the deque's real capacity, for the life
        # of the node (worse than most other stale-config bugs here, since
        # not even a restart would need to be involved for the value to
        # look "live" - it just silently never mattered). Capacity is now
        # enforced manually below, re-reading self.max_size every item, so
        # it's actually live-reconfigurable. `drop_policy` was also cached
        # but never read again - 'block' is now implemented as "reject the
        # incoming item and keep the queue as-is" (the closest non-blocking
        # equivalent available in this single-consumer design); 'drop' (or
        # anything else) keeps the original behavior of evicting the
        # oldest item to make room for the new one.
        self.queue = deque()

    async def reset(self):
        # See the identical note on StackNode.reset(): the generic
        # BaseNode.reset() only clears stats/error bookkeeping, so the
        # actual accumulated queue needs its own clear here.
        await super().reset()
        self.queue.clear()

    def _enforce_capacity(self, incoming):
        max_size = self.max_size
        if max_size <= 0:
            return True
        if len(self.queue) < max_size:
            return True
        if self.drop_policy == 'block':
            return False
        while len(self.queue) >= max_size:
            self.queue.popleft()
        return True

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
                if self._enforce_capacity(item):
                    self.queue.append(item)
            if got_one:
                self.emit('out', {'queue': list(self.queue), 'size': len(self.queue)})
            elif self.queue:
                # Emit current state periodically even with no new item,
                # matching the previous behavior on a lone timeout.
                self.emit('out', {'queue': list(self.queue), 'size': len(self.queue)})
