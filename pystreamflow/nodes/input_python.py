import asyncio
import contextlib
import io
from datetime import datetime, timezone

from ..core.exec_guard import script_execution_allowed
from ..core.node import BaseNode


class PythonScriptInputNode(BaseNode):
    """Python Script Input Node.

    Periodically executes a real Python snippet (``config['script']``)
    via ``exec()`` and emits whatever it printed to stdout - or, if the
    script sets a ``result`` variable instead of printing, that value -
    not a canned sample.

    Uses the same dual-execution strategy as ``ScriptedOutputNode``
    (``pystreamflow/nodes/scripted_output.py``): the script is first run
    as top-level statements (so ``result = ...``-style scripts work),
    falling back on ``SyntaxError`` to wrapping it as a zero-argument
    function body and calling that (so a bare ``return ...``-style
    script also works). Reused here rather than reinvented, since it's
    the same execution semantics applied to a source node instead of a
    per-item transform.

    TRUST BOUNDARY: identical to ``ScriptedOutputNode``'s (see that
    class's docstring for the full disclosure) - ``exec()`` runs with the
    full privileges of this process, driven by config that isn't
    necessarily trusted. Gated by the same ``PSF_ALLOW_SCRIPT_NODES`` env
    var (``pystreamflow/core/exec_guard.py``); set it to ``0`` to
    hard-disable this node's execution path.

    Config:
      script: Python source to run each cycle (default ``'print("hi")'``)
      interval: seconds between runs, ``0`` = run once (default ``2.0``)
    """

    async def init(self):
        self.script = self.config.get('script', 'print("hi")')
        self.interval = float(self.config.get('interval', 2.0))

    async def process(self):
        while self._running:
            await self._run_once()
            if self.interval > 0:
                await asyncio.sleep(self.interval)
            else:
                break

    async def _run_once(self):
        timestamp = datetime.now(timezone.utc).isoformat()
        if not script_execution_allowed():
            msg = 'script execution disabled (PSF_ALLOW_SCRIPT_NODES=0)'
            self._last_error = msg
            self.emit('out', {'script': self.script, 'error': msg, 'timestamp': timestamp})
            return

        local_ns = {}
        stdout_buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(stdout_buf):
                try:
                    exec(self.script, {}, local_ns)
                except SyntaxError:
                    code = "def __script__():\n"
                    for line in self.script.splitlines():
                        code += f"    {line}\n"
                    exec(code, {}, local_ns)
                    fn = local_ns.get('__script__')
                    if callable(fn):
                        local_ns['result'] = fn()
        except Exception as e:
            self._last_error = str(e)
            self.emit('out', {'script': self.script, 'error': str(e), 'timestamp': timestamp})
            return

        printed = stdout_buf.getvalue()
        output = printed if printed else local_ns.get('result')
        self.emit('out', {'script': self.script, 'output': output, 'timestamp': timestamp})
