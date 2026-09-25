from ..core.node import BaseNode
import asyncio
import re

_IN_PORT_RE = re.compile(r'^in(\d+)$')


class LedActivityNode(BaseNode):
    """A visual pass-through "patch bay": every wired input is relayed,
    completely unchanged, to the output at the same index (``in0`` ->
    ``out0``, ``in1`` -> ``out1``, ...). The node editor's "+ port"/"−
    port" buttons for this node type grow or shrink a matched
    ``inN``/``outN`` pair together (see api/static/editor.js's
    ``psfPairedInOut``), and give it a small blinking LED indicator that
    lights up on every relay - a real patch panel's activity light, more
    of a wiring/diagnostic gadget than a data transform.
    """

    async def init(self):
        pass

    @staticmethod
    def _matching_output(name: str) -> str:
        m = _IN_PORT_RE.match(name)
        return f'out{m.group(1)}' if m else name

    async def process(self):
        while self._running:
            # Re-fetched every outer pass (not captured once) so a port
            # wired after this node was created and auto-started - the
            # normal ad-hoc node-editor sequence - is picked up within one
            # pass instead of never, matching the late-wire fix pattern
            # already used throughout this codebase.
            pipes = dict(self.inputs)
            if not pipes:
                await asyncio.sleep(0.2)
                continue
            got_one = False
            for name, pipe in pipes.items():
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue
                got_one = True
                self.emit(self._matching_output(name), item)
            if not got_one:
                await asyncio.sleep(0.05)
