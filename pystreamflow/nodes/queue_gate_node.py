from ..core.node import BaseNode
import asyncio
from collections import deque


class QueueGateNode(BaseNode):
    """A token-gated relay queue: every item received on ``in`` is queued;
    every time ``pop`` receives *anything at all* (its actual content is
    discarded - only its arrival matters), exactly one queued item is
    popped off, in FIFO order, and relayed to ``out``.

    If the queue is empty when a pop trigger arrives, that trigger is
    simply a no-op - there's nothing to relay yet - rather than being
    remembered to release the *next* enqueued item early. It's always a
    queued item being gated by a pop signal, never the other way around.
    """

    async def init(self):
        self.max_size = int(self.config.get('max_size', 0))  # 0 = unbounded
        self._queue: deque = deque()

    async def process(self):
        while self._running:
            # Re-fetched every outer pass (not captured once) so a wire
            # drawn onto either port after this node was created and
            # auto-started - the normal ad-hoc node-editor sequence - is
            # picked up within one pass instead of never, matching the
            # late-wire fix pattern already used throughout this codebase.
            in_pipe = self.inputs.get('in')
            pop_pipe = self.inputs.get('pop')
            if not in_pipe and not pop_pipe:
                await asyncio.sleep(0.2)
                continue
            tasks = {}
            if in_pipe:
                tasks['in'] = asyncio.create_task(in_pipe.get())
            if pop_pipe:
                tasks['pop'] = asyncio.create_task(pop_pipe.get())
            done, _pending = await asyncio.wait(
                list(tasks.values()), timeout=0.5, return_when=asyncio.FIRST_COMPLETED
            )
            for name, task in tasks.items():
                if task not in done:
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
                    continue
                try:
                    item = task.result()
                except Exception:
                    continue
                if name == 'in':
                    self._queue.append(item)
                    if self.max_size > 0:
                        while len(self._queue) > self.max_size:
                            self._queue.popleft()
                else:  # 'pop' - content discarded, only arrival matters
                    if self._queue:
                        self.emit('out', self._queue.popleft())
