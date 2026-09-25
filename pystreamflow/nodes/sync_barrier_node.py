from ..core.node import BaseNode
import asyncio
import re

_IN_PORT_RE = re.compile(r'^in(\d+)$')


class SyncBarrierNode(BaseNode):
    """A variable-input barrier/sync join: waits until *every* currently
    wired input has received at least one message, then releases them all
    at once - each held value emitted on its own matching output
    (``in0`` -> ``out0``, ``in1`` -> ``out1``, ...) in the same pass - and
    optionally flushes (clears) what it was holding afterward.

    The node editor's "+ port"/"− port" buttons for this node type grow or
    shrink a matched ``inN``/``outN`` pair together, so the number of
    inputs and the number of outputs never drift apart (see
    api/static/editor.js's ``psfPairedInOut``).

    ``flush`` (config, default ``True``) controls what happens to a held
    value after it's released:
      - ``True``  (default, "one full round per release"): every emitted
        value is discarded immediately after release, so *every* input
        must contribute a genuinely new message before the next release -
        classic barrier/zip semantics.
      - ``False`` ("hold the latest, re-emit on any update"): once every
        input has contributed at least once, held values are kept rather
        than cleared - so from then on, a new message on *any single*
        input immediately triggers another release of the full set (the
        other outputs' held values may be "stale" relative to their own
        input, by design) - combine-latest semantics, for a workflow that
        wants "the current full picture, refreshed whenever anything
        changes" rather than "wait for a fresh reading from everything
        every single time".
    """

    async def init(self):
        self.flush = bool(self.config.get('flush', True))
        self._pending: dict = {}

    @staticmethod
    def _matching_output(name: str) -> str:
        m = _IN_PORT_RE.match(name)
        return f'out{m.group(1)}' if m else name

    async def process(self):
        while self._running:
            # Re-fetched every outer pass (not captured once) so an input
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
                self._pending[name] = item
                got_one = True
            if not got_one:
                continue
            if all(name in self._pending for name in pipes):
                for name, value in list(self._pending.items()):
                    self.emit(self._matching_output(name), value)
                if self.flush:
                    self._pending.clear()
