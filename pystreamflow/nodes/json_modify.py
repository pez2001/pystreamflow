from ..core.node import BaseNode
import asyncio
import json

class JSONModifyNode(BaseNode):
    async def init(self):
        self.ops = self.config.get('ops', [])
    async def process(self):
        # `pipe` re-fetches self.inputs.get('in') every pass (rather than
        # once before the loop) so a port wired after this node starts is
        # actually noticed - see the identical note in
        # core/transform_node.py/encoding_convert.py.
        while self._running:
            pipe = self.inputs.get('in')
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    try:
                        data = json.loads(str(item)) if isinstance(item, (str, bytes)) else item
                    except Exception:
                        data = item
                    # Apply simple ops: set, rename, delete. This whole
                    # block used to be uncaught: an op that didn't match
                    # the shape of `data` (e.g. a 'set' whose path walks
                    # through a non-dict value left behind by an earlier
                    # op, or a 'delete' path with a missing intermediate
                    # key) raised AttributeError/TypeError straight out of
                    # process() - which BaseNode._run_loop's default
                    # retries=0 turns into permanently killing the node on
                    # the very first malformed item. Now a bad op is
                    # recorded via _last_error and skipped, and the
                    # already-applied ops still take effect on 'out'.
                    for op in self.ops:
                        try:
                            action = op.get('action')
                            if action == 'set':
                                path = op.get('path','').split('.')
                                d = data
                                for k in path[:-1]:
                                    d = d.setdefault(k, {})
                                d[path[-1]] = op.get('value')
                            elif action == 'rename':
                                old = op.get('from')
                                new = op.get('to')
                                if isinstance(data, dict) and old in data:
                                    data[new] = data.pop(old)
                            elif action == 'delete':
                                keys = op.get('path','').split('.')
                                d = data
                                for k in keys[:-1]:
                                    d = d.get(k, {})
                                d.pop(keys[-1], None)
                        except Exception as e:
                            self._last_error = f'op {op!r} failed: {e}'
                    self.emit('out', data)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
