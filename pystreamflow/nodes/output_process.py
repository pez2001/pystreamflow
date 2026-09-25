import asyncio
import shlex

from ..core.exec_guard import command_execution_allowed
from ..core.node import BaseNode
from ..core.paths import data_dir, ensure_dir
from ..core.subprocess_exec import run_subprocess


class ProcessOutputNode(BaseNode):
    """Process Output Node.

    Pipes each incoming item to a real OS command's stdin
    (``config['cmd']``) and emits the command's actual stdout - not an
    echo of the node's own config.

    Also fixes a wiring bug shared with the pre-fix ``SocketOutputNode``
    and ``ScriptNode``: the original ``process()`` only ever checked
    ``self.inputs.get('in')`` once, before entering its loop, so a node
    started before anything wired its ``'in'`` port would sit in the
    ``else: sleep`` branch forever even after a real pipe was wired in
    afterward. This now waits for the port to appear (matching the
    pattern already used by ``ScriptedOutputNode``).

    TRUST BOUNDARY: ``cmd`` is split with ``shlex`` and exec'd directly
    (no shell); see ``ProcessInputNode``'s docstring for the full
    trust-boundary disclosure (identical mechanism, reused here). Gated
    by ``PSF_ALLOW_SHELL_NODES`` (``pystreamflow/core/exec_guard.py``);
    set it to ``0`` to hard-disable this node's execution path.

    Config:
      cmd: command line, e.g. ``'cat'`` (default)
      timeout: seconds allowed per run (default ``30.0``)
      cwd: working directory for the command (default: ``data_dir()`` -
        ``PSF_DATA_DIR``, ``/app/data`` under the packaged
        docker-compose.yml). This was previously missing from this node
        type entirely - unlike ``ProcessInputNode``/``ShellInputNode``,
        it never passed a ``cwd`` to the subprocess at all, so a
        relative filename in ``cmd`` (e.g. ``'tee report.txt'``) landed
        wherever this process's own cwd happened to be rather than
        somewhere consistent and visible to the operator; created if it
        doesn't exist yet.
    """

    async def init(self):
        self.cmd = self.config.get('cmd', 'cat')
        self.timeout = float(self.config.get('timeout', 30.0))
        self.cwd = self.config.get('cwd') or data_dir()
        try:
            ensure_dir(self.cwd)
        except OSError as e:
            self._last_error = f'could not create cwd {self.cwd!r}: {e}'

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
            await self._run_for_item(item)

    async def _run_for_item(self, item):
        if not command_execution_allowed():
            msg = 'command execution disabled (PSF_ALLOW_SHELL_NODES=0)'
            self._last_error = msg
            self.emit('out', {'cmd': self.cmd, 'input': item, 'error': msg})
            return

        try:
            args = shlex.split(self.cmd)
        except ValueError as e:
            self._last_error = str(e)
            self.emit('out', {'cmd': self.cmd, 'input': item, 'error': str(e)})
            return
        if not args:
            return

        result = await run_subprocess(args, shell=False, cwd=self.cwd, timeout=self.timeout, input_data=str(item))
        if 'error' in result:
            self._last_error = result['error']
            self.emit('out', {'cmd': self.cmd, 'input': item, 'error': result['error']})
            return

        self.emit('out', {
            'cmd': self.cmd,
            'input': item,
            'output': result['stdout'],
            'returncode': result['returncode'],
        })
