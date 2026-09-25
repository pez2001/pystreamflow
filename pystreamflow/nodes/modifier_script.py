import asyncio

from ..core.exec_guard import script_execution_allowed
from ..core.node import BaseNode
from ..core.stream import Pipe


class ScriptNode(BaseNode):
    """Script Node.

    Runs a real user-supplied Python snippet (``config['script']``)
    against every item this node receives, via ``exec()`` - not a fixed
    ``f'scripted:{item}'`` string.

    This is the same execution strategy as ``ScriptedOutputNode``
    (``pystreamflow/nodes/scripted_output.py``), reused rather than
    reinvented: the script is first run as top-level statements (so a
    ``result = ...``-style script works, reading ``data`` for the
    incoming item), falling back on ``SyntaxError`` to wrapping it as a
    one-argument function body and calling it with the item (so a bare
    ``return ...``-style script also works).

    TRUST BOUNDARY: identical to ``ScriptedOutputNode``'s (see that
    class's docstring for the full disclosure) - ``exec()`` runs with the
    full privileges of this process, driven by config that isn't
    necessarily trusted. Gated by ``PSF_ALLOW_SCRIPT_NODES``
    (``pystreamflow/core/exec_guard.py``); set it to ``0`` to
    hard-disable this node's execution path (it emits a clear error item
    instead of running the script, rather than failing silently).

    Config:
      script: Python source, e.g. ``'result = data.upper()'`` (default ``'pass'``)
    """

    async def init(self):
        self.script = self.config.get('script', 'pass')
        # Phase 6 hardening pattern (see BaseNode.add_output_if_unwired()):
        # only create our own internal pipe if nothing has wired 'out' yet,
        # so a real Engine-wired downstream pipe is never clobbered.
        self.out_pipe = self.add_output_if_unwired('out', Pipe())

    async def process(self):
        pipe = self.inputs.get('in')
        if not pipe:
            while self._running and 'in' not in self.inputs:
                await asyncio.sleep(0.5)
            pipe = self.inputs.get('in')
        if not pipe:
            return

        while self._running:
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            if not script_execution_allowed():
                out_item = {
                    'script': self.script,
                    'input': item,
                    'error': 'script execution disabled (PSF_ALLOW_SCRIPT_NODES=0)',
                }
                self._last_error = out_item['error']
                self.emit('out', out_item)
                continue

            try:
                local_ns = {'data': item}
                try:
                    exec(self.script, {}, local_ns)
                    output = local_ns.get('result')
                except SyntaxError:
                    code = "def __script__(data):\n"
                    for line in self.script.splitlines():
                        code += f"    {line}\n"
                    exec(code, {}, local_ns)
                    fn = local_ns.get('__script__')
                    output = fn(item) if callable(fn) else None
                out_item = {'script': self.script, 'input': item, 'output': output}
            except Exception as e:
                out_item = {'script': self.script, 'input': item, 'error': str(e)}
                self._last_error = str(e)

            self.emit('out', out_item)
