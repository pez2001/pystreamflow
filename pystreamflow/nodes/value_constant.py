from ..core.node import BaseNode
import asyncio


class ConstantValueNode(BaseNode):
    """Emits exactly one configured literal value, with no metadata
    wrapper at all - the "clean raw string" source the attribute-wiring
    feature was missing (see the project's evaluation doc, finding 3d.5):
    every other source-like node in this library wraps its real payload
    in a dict (``ListStringsNode``/``GeneratorInputNode`` emit
    ``{'value': ..., 'index': ...}``, ``TimerNode`` emits
    ``{'tick', 'timestamp', 'payload'}``), so wiring one of those into,
    say, ``FileInputNode``'s ``path`` attribute set that attribute to a
    dict, not the bare filepath string a user actually wants there.

    ``config['value']`` is emitted exactly as configured - a string,
    number, or bool straight out of the workflow/config JSON, never
    reshaped into ``{'value': ...}`` the way most of this package's other
    source nodes do. Meant primarily as the source end of an
    ``attribute``-type edge (see ``core/node.py``'s
    ``BaseNode.set_attribute()`` and ``core/engine.py``'s
    ``Engine._pump_attribute()``) - e.g. wiring a real path onto a
    ``FileInputNode``'s ``path`` - but it emits on the ``out`` port like
    any other source, so it also works as a normal ``data`` edge source.

    By default the value is emitted once, immediately, and the node then
    idles (matching "set this attribute once, at start" - the common
    case for something like a fixed filepath). Set ``config['repeat']``
    to re-emit the same value every ``config['interval']`` seconds
    instead, for a value that needs to be (re-)asserted periodically.
    """

    async def init(self):
        self.value = self.config.get('value', '')
        self.repeat = bool(self.config.get('repeat', False))
        self.interval = float(self.config.get('interval', 1.0))

    async def process(self):
        self.emit('out', self.value)
        if not self.repeat:
            return
        while self._running:
            await asyncio.sleep(self.interval)
            self.emit('out', self.value)
