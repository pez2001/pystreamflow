from ..core.node import BaseNode
import asyncio

class AndNode(BaseNode):
    async def init(self):
        self.inputs_needed = int(self.config.get('inputs_needed', 2))
    async def process(self):
        # Wait for inputs and AND them
        #
        # Regression fix: this used to collect one round of values and
        # emit exactly once, then return - process() completing meant the
        # node's background task was simply done, so an AndNode (or any
        # of its Or/Not/Nand/Nor/Xor/Xnor siblings - all had the identical
        # bug) only ever produced a single result for its entire lifetime
        # and then silently stopped responding to any further input.
        # Wrapping the evaluation in `while self._running:` makes it
        # behave like every other node type in this library: keep
        # evaluating and emitting for as long as the node runs.
        #
        # Second bug fix (found live, from a direct report against a
        # different node type with the identical shape - modifier_json.py/
        # JSONExtractNode - but affecting this whole boolean-gate family
        # too): `pipes` used to also be captured once, *outside* this
        # loop - a wire arriving via POST /nodes/connect after this node
        # was created and auto-started (the normal ad-hoc node-editor
        # sequence) updates self.inputs but never that already-captured
        # list, so a late-wired input was never seen at all. Re-fetching
        # it every outer iteration picks up newly-wired inputs within one
        # pass instead of never.
        while self._running:
            pipes = list(self.inputs.values())
            if not pipes:
                await asyncio.sleep(0.5)
                continue
            values = []
            for p in pipes:
                try:
                    item = await asyncio.wait_for(p.get(), timeout=0.1)
                    values.append(bool(item))
                except asyncio.TimeoutError:
                    continue
            if len(values) >= self.inputs_needed:
                result = all(values[:self.inputs_needed])
                self.emit('out', result)
