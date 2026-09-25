from ..core.node import BaseNode
import asyncio
import json

class JSONOutputNode(BaseNode):
    async def init(self):
        pretty = self.config.get('pretty', True)
        # A raw bool(...) here would make the *string* "false" (as a UI
        # text-widget or YAML value might hand in) evaluate truthy, since
        # any non-empty string is truthy in Python - silently ignoring a
        # config value that clearly meant "off".
        if isinstance(pretty, str):
            self.pretty = pretty.strip().lower() not in ('false', '0', 'no', 'off', '')
        else:
            self.pretty = bool(pretty)

    async def process(self):
        # `pipe` re-fetches self.inputs.get('in') every pass - see the
        # identical note in core/transform_node.py/encoding_convert.py.
        while self._running:
            pipe = self.inputs.get('in')
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    try:
                        out = json.dumps(item, indent=2) if self.pretty else json.dumps(item)
                    except TypeError as e:
                        # A non-JSON-serializable item (bytes, a custom
                        # object, ...) used to raise uncaught here,
                        # propagating out of process() and - with
                        # BaseNode._run_loop's default retries=0 -
                        # permanently killing the node on the very first
                        # such item. Fall back to a best-effort string
                        # representation instead.
                        self._last_error = f'not JSON-serializable: {e}'
                        out = json.dumps(str(item), indent=2 if self.pretty else None)
                    self.emit('out', out)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
