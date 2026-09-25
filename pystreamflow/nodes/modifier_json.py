from ..core.node import BaseNode
import asyncio
import json
import logging

logger = logging.getLogger("pystreamflow.node.json_extract")


class JSONExtractNode(BaseNode):
    async def init(self):
        self.path = self.config.get('path', '')

    def _extract(self, item):
        """Parse `item` into a JSON-shaped Python value and, if `path` is
        set, walk it dot-by-dot down to the target value.

        Bug fix (found live, from a direct report: "connected api input
        out to json extract in, set path to 'msg', connected json extract
        out to display - the message went through to the display node
        unchanged"): this used to unconditionally do
        `json.loads(str(item))`, no matter what `item` actually was. Any
        upstream node that already hands this node a real Python dict/
        list - ApiInputNode's parsed request body, WebInputNode's parsed
        JSON body, and in fact most of this codebase's own nodes - means
        `str(item)` is Python's *repr* of that value: single-quoted keys,
        `None`/`True`/`False` instead of JSON's `null`/`true`/`false`.
        That is not valid JSON, so `json.loads()` on it always raised,
        and the blind `except Exception: self.emit('out', item)` this was
        wrapped in silently re-emitted the *original, unextracted* item
        every single time - so for any real upstream node whose output
        wasn't a literal JSON *string* (the only case the existing tests
        ever fed it), this node did nothing but pass its input straight
        through, with zero indication anywhere that extraction never
        actually ran.

        Also hardened the per-key walk itself: the old
        `data = data.get(k, {})` loop assumed `data` was always a dict at
        every step and would raise `AttributeError` (silently swallowed
        the same way) the moment a path walked into a list or a scalar.
        Dict keys and list indices are both handled below; a path segment
        that can't be resolved against what's actually there now yields
        `{}` for that step, consistent with the original dict-only
        behavior's own "key not found" fallback, rather than blowing up
        the whole extraction and reverting to passthrough.
        """
        data = item if isinstance(item, (dict, list)) else json.loads(str(item))
        if self.path:
            for key in self.path.split('.'):
                if isinstance(data, dict):
                    data = data.get(key, {})
                elif isinstance(data, list):
                    try:
                        data = data[int(key)]
                    except (ValueError, IndexError):
                        data = {}
                else:
                    data = {}
        return data

    async def process(self):
        # Bug fix (found live: even after the json.loads()-on-a-dict fix
        # above, a node wired via POST /nodes/connect *after* creation -
        # the normal ad-hoc node-editor sequence now that new nodes
        # auto-start immediately - received nothing at all, forever).
        # `pipe` re-fetches self.inputs.get('in') every pass instead of
        # once before the loop, matching the identical, already-correct
        # pattern in json_modify.py/json_output.py/line_buffer.py, so a
        # port wired after this node starts is actually noticed.
        while self._running:
            pipe = self.inputs.get('in')
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    try:
                        data = self._extract(item)
                        self.emit('out', data)
                    except Exception:
                        logger.warning(
                            "node %s: could not extract path %r from %r - emitting the item unchanged",
                            self.id, self.path, item,
                        )
                        self.emit('out', item)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
