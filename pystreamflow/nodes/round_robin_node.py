from ..core.node import BaseNode
import asyncio


class RoundRobinNode(BaseNode):
    """Distributes one input stream across a fixed, configured number of
    numbered output ports (``out0``, ``out1``, ...), one at a time, in a
    strict repeating cycle: the 1st item received goes to ``out0``, the
    2nd to ``out1``, ... the Nth (``count``-th) item goes to
    ``out{count-1}``, and the (N+1)th item wraps back around to ``out0``
    again.

    Feature request, verbatim: "add a node to feed inputs to multiple
    outputs and it cycles through all outputs (first input message gets
    to the first output, the second incoming message goes to the second
    output. if all outputs received one message start the cycle again)".

    ``count`` (default 3) sets how many output ports exist, matching the
    editor's own "+ output"/"− output" buttons for this node type (see
    api/static/editor.js's psfRoundRobin branch, which mirrors how
    SplitByValueNode keeps its own `values` config and numbered output
    ports in lock-step - see modifier_split_by_value.py). The cycle
    position advances by exactly one for every item received, regardless
    of whether the port whose turn it is happens to be wired right now -
    matching the request's own plain "1st -> 1st output, 2nd -> 2nd
    output" phrasing (a strict positional assignment that keeps every
    output's *turn* fixed) rather than silently skipping unwired ports to
    keep every currently-wired consumer equally fed. If that skip-unwired
    behavior is what's wanted instead, wire every output before sending
    real traffic, or use fewer outputs.

    Unlike ForkNode (duplicates one item to *every* wired output at once)
    or SplitByValueNode (routes by the item's own value), this node
    doesn't look at the item at all - purely turn-taking by arrival order,
    the classic round-robin load-balancing pattern.
    """

    async def init(self):
        # Clamped to at least 1 - "cycle across zero outputs" is
        # meaningless, and mod-ing by zero would crash on the very first
        # item, so a bad/blank config value degrades to "always out0"
        # rather than an unrecoverable node.
        self.count = max(1, int(self.config.get('count', 3)))
        self._next = 0

    async def reset(self):
        # Generic BaseNode.reset() only clears stats/error bookkeeping -
        # this node's whole point is *where in the cycle it currently
        # is*, so a reset control message needs to restart the cycle at
        # out0 too, or it wouldn't actually reset anything a user of this
        # node would notice (same override pattern StackNode/
        # FIFOQueueNode/LIFOQueueNode/TableNode/LineBufferNode already
        # use - see core/node.py's reset() docstring).
        await super().reset()
        self._next = 0

    async def process(self):
        while self._running:
            # Re-fetched every outer pass (not captured once) so a wire
            # drawn onto 'in' after this node was created and auto-started
            # - the normal ad-hoc node-editor sequence - is picked up
            # within one pass instead of never, matching the late-wire fix
            # pattern already used throughout this codebase (see e.g.
            # modifier_fork.py's own comment).
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            self.emit(f'out{self._next}', item)
            self._next = (self._next + 1) % self.count
