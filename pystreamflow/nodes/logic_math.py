from ..core.node import BaseNode
import asyncio
import operator

class MathNode(BaseNode):
    async def init(self):
        self.op = self.config.get('op', 'add')
        self.value = self.config.get('value', 0)
        self.ops = {
            'add': operator.add,
            'sub': operator.sub,
            'mul': operator.mul,
            'div': operator.truediv,
            'mod': operator.mod,
            'pow': operator.pow,
        }

    async def process(self):
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipe` used to be captured once, before this
        # loop even started - a wire arriving via POST /nodes/connect
        # after this node was created and auto-started (the normal ad-hoc
        # node-editor sequence) was never seen. Re-fetching `pipe` every
        # outer iteration - and looping instead of returning when unwired -
        # picks up a late wire within one pass instead of never.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                try:
                    # `func` used to be resolved from self.op once, before
                    # this loop started, so a live attribute-wire update to
                    # `op` (e.g. from a settings/list node) updated
                    # self.op/self.config['op'] correctly but the running
                    # node kept applying the original operator forever -
                    # and worse, the int-cast check below already re-read
                    # the live self.op, so the two could disagree after a
                    # live change. Fixed by re-resolving func from the
                    # live self.op on every item.
                    func = self.ops.get(self.op, operator.add)
                    a = float(item)
                    b = float(self.value)
                    result = func(a, b)
                    # Keep int if both ints
                    if isinstance(item, int) and isinstance(self.value, int) and self.op in ('add','sub','mul','mod','pow'):
                        result = int(result)
                    self.emit('out', result)
                except Exception:
                    self.emit('out', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
                continue
