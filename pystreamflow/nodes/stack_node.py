from ..core.node import BaseNode
import asyncio

class StackNode(BaseNode):
    async def init(self):
        self.max_size = int(self.config.get('max_size', 1000))
        self.mode = self.config.get('mode', 'push_pop')  # push_pop, peek, clear
        # Was a local variable inside process() - BaseNode.pause()'s own
        # docstring promises that state kept on self survives a pause()/
        # resume() cycle, but pause()/resume() cancel and re-create the
        # process() task (see node.py), which re-executed `stack = []`
        # from scratch every time, silently wiping the node's entire
        # stack on every pause/resume. Caching it on self fixes that.
        self.stack = []

    async def reset(self):
        # Generic BaseNode.reset() only clears stats/error bookkeeping -
        # this node's whole point is the accumulated `stack`, so a
        # reset control message needs to clear that too, or it wouldn't
        # actually reset anything a user of this node would notice.
        await super().reset()
        self.stack.clear()

    async def process(self):
        # Bug fix (found during a codebase-wide audit for this exact
        # shape of problem): port_schema.py deliberately declares this
        # node's inputs as DYNAMIC and documents StackNode as one of its
        # "true multi-input" node types, in the same category as AndNode/
        # MergeNode - which really do poll every wired input pipe each
        # pass. This node instead only ever read from ONE arbitrary pipe
        # (`next(iter(self.inputs.values()), None)`, whichever happened
        # to be wired first) and silently never looked at any other input
        # wired under a different port name - the exact "silently inert
        # wiring" failure mode port_schema.py's own module docstring
        # exists to prevent. Iterating every currently-wired pipe each
        # outer pass (the same pattern AndNode/MergeNode already use)
        # fixes this while leaving single-input use (by far the common
        # case) completely unchanged.
        while self._running:
            pipes = list(self.inputs.values())
            if not pipes:
                await asyncio.sleep(0.1)
                continue
            for pipe_in in pipes:
                try:
                    item = await asyncio.wait_for(pipe_in.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                if self.mode == 'push_pop':
                    if len(self.stack) >= self.max_size:
                        self.stack.pop(0)
                    self.stack.append(item)
                    # Emit top of stack
                    if self.stack:
                        self.emit('out', {'stack': list(self.stack), 'top': self.stack[-1]})
                elif self.mode == 'peek':
                    # A real peek must not mutate the stack - it used to do
                    # the exact same push+evict as 'push_pop' above, making
                    # the two modes behave identically. The incoming item is
                    # still consumed as the "peek now" trigger; it's just not
                    # pushed onto the stack.
                    self.emit('out', {'stack': list(self.stack), 'top': self.stack[-1] if self.stack else None})
                elif self.mode == 'clear':
                    self.stack.clear()
                    self.emit('out', {'stack': []})
