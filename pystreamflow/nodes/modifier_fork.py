from ..core.node import BaseNode
import asyncio

class ForkNode(BaseNode):
    async def init(self):
        pass
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
                # Fork to every wired output, whatever it's named -
                # port_schema.py declares this node's outputs DYNAMIC, so
                # this loop is what makes the editor's paired 'outN'/'rawN'
                # ports (see api/static/editor.js's psfPairedRawOutputs)
                # work with no special-casing here: a wired 'raw0' port is
                # just another entry in self.outputs and gets the exact
                # same duplicated item as 'out0' does, automatically.
                for out_name in self.outputs:
                    self.emit(out_name, item)
            except asyncio.TimeoutError:
                continue
