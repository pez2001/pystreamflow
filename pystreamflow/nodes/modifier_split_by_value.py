from ..core.node import BaseNode
import asyncio


class SplitByValueNode(BaseNode):
    """Routes each incoming item to a different output port depending on
    its value - or, if ``key`` is set, one dot-separated field of it (the
    same path syntax ``JSONExtractNode`` uses, see modifier_json.py's
    ``_extract()``) - rather than passing everything straight through.
    ``values[i]`` matches output ``out{i}``; anything that matches none of
    them goes to the always-present ``default`` output instead, so nothing
    is ever silently dropped just because it didn't match a configured
    value.

    Feature request: "split by value into outputs nodes (make value-
    >output attributes expandable in number, add output accordingly)" -
    the node editor's dedicated "+ value/output" and "− value/output"
    buttons for this node type (see api/static/editor.js) grow or shrink
    ``values`` and its matching numbered output port together, one pair at
    a time, so the value->output mapping is always a real, visible,
    editable list on the canvas rather than a hidden index a person has to
    keep in sync by hand.
    """

    async def init(self):
        self.values = list(self.config.get('values', []))
        self.key = self.config.get('key', '')
        # Config values arrive from the editor's plain comma-separated
        # text widget as strings, so a workflow matching against a
        # *numeric* upstream value (e.g. the integer 404) would never
        # match the literal string "404" typed into that field under
        # strict, typed equality. Stringified comparison is the forgiving
        # default that actually works for the common case; `strict: true`
        # opts back into real typed equality for anyone who wants it.
        self.strict = bool(self.config.get('strict', False))

    def _extract_key(self, item):
        """Mirrors JSONExtractNode._extract()'s dot-path walk (see
        modifier_json.py) but never raises - an unresolvable path segment
        just yields ``{}`` for that step, same as that node's own
        fallback, since a routing decision should degrade to "no match"
        rather than crash the node.
        """
        if not self.key:
            return item
        data = item
        for part in self.key.split('.'):
            if isinstance(data, dict):
                data = data.get(part, {})
            elif isinstance(data, list):
                try:
                    data = data[int(part)]
                except (ValueError, IndexError):
                    data = {}
            else:
                data = {}
        return data

    def _matches(self, candidate, target) -> bool:
        if self.strict:
            return candidate == target
        return str(candidate) == str(target)

    async def process(self):
        # `pipe` is re-fetched every outer pass rather than captured once
        # before the loop - the same late-wire fix already applied
        # throughout this codebase (see e.g. modifier_fork.py's own
        # comment) - so a wire drawn after this node was created and
        # auto-started is still picked up.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            candidate = self._extract_key(item)
            for i, target in enumerate(self.values):
                if self._matches(candidate, target):
                    self.emit(f'out{i}', item)
                    break
            else:
                self.emit('default', item)
