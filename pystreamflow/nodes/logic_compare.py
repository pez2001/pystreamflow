from ..core.node import BaseNode
import asyncio

class CompareNode(BaseNode):
    async def init(self):
        self.operator = self.config.get('operator', 'eq')
        self.compare_value = self.config.get('compare_value')

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
                result = self._compare(item, self.compare_value)
                self.emit('out', result)
                # Also emit true/false branches if configured
                if self.config.get('branch_on_result', False):
                    branch = 'true' if result else 'false'
                    self.emit(branch, item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
                continue

    def _compare(self, a, b):
        op = self.operator
        try:
            if op == 'eq':
                return a == b
            elif op == 'ne':
                return a != b
            elif op == 'gt':
                return a > b
            elif op == 'lt':
                return a < b
            elif op == 'gte':
                return a >= b
            elif op == 'lte':
                return a <= b
            elif op == 'contains':
                return b in str(a)
            else:
                return False
        except Exception:
            return False
