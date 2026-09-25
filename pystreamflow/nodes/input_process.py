import asyncio
import shlex
from datetime import datetime, timezone

from ..core.exec_guard import command_execution_allowed
from ..core.node import BaseNode
from ..core.paths import data_dir, ensure_dir
from ..core.subprocess_exec import run_subprocess


class ProcessInputNode(BaseNode):
    """Process Input Node.

    Periodically runs a real OS command (``config['cmd']``) and emits its
    actual stdout/stderr/returncode - not a canned sample.

    TRUST BOUNDARY: ``cmd`` is split with ``shlex`` and exec'd directly
    (no shell involved, so shell metacharacters like ``|``/``>``/``;``
    are inert - use ``ShellInputNode`` if those are needed), but it is
    still arbitrary program execution with this process's own
    privileges, driven by workflow config that - like every other node's
    config - can come from an unauthenticated HTTP API call
    (``pystreamflow/api/server.py`` has no authentication) or a saved
    workflow file. Gated by ``PSF_ALLOW_SHELL_NODES``
    (``pystreamflow/core/exec_guard.py``); set it to ``0`` to
    hard-disable this node's execution path (it emits a clear error item
    instead of running the command, rather than failing silently).

    Config:
      cmd: command line, e.g. ``'echo hello'`` (default)
      interval: seconds between runs, ``0`` = run once (default ``2.0``)
      timeout: seconds allowed per run (default ``30.0``)
      cwd: working directory for the command (default: ``data_dir()`` -
        ``PSF_DATA_DIR``, ``/app/data`` under the packaged
        docker-compose.yml - rather than this process's own container-
        internal cwd, so a command like ``'cat report.csv'`` reads/writes
        somewhere the operator can actually see by default; created if
        it doesn't exist yet)
    """

    async def init(self):
        self.cmd = self.config.get('cmd', 'echo hello')
        self.interval = float(self.config.get('interval', 2.0))
        self.timeout = float(self.config.get('timeout', 30.0))
        self.cwd = self.config.get('cwd') or data_dir()
        try:
            ensure_dir(self.cwd)
        except OSError as e:
            self._last_error = f'could not create cwd {self.cwd!r}: {e}'

    async def process(self):
        while self._running:
            await self._run_once()
            if self.interval > 0:
                await asyncio.sleep(self.interval)
            else:
                break

    async def _run_once(self):
        timestamp = datetime.now(timezone.utc).isoformat()
        if not command_execution_allowed():
            msg = 'command execution disabled (PSF_ALLOW_SHELL_NODES=0)'
            self._last_error = msg
            self.emit('out', {'cmd': self.cmd, 'error': msg, 'timestamp': timestamp})
            return

        try:
            args = shlex.split(self.cmd)
        except ValueError as e:
            self._last_error = str(e)
            self.emit('out', {'cmd': self.cmd, 'error': str(e), 'timestamp': timestamp})
            return
        if not args:
            return

        result = await run_subprocess(args, shell=False, cwd=self.cwd, timeout=self.timeout)
        if 'error' in result:
            self._last_error = result['error']
            self.emit('out', {'cmd': self.cmd, 'error': result['error'], 'timestamp': timestamp})
            return

        self.emit('out', {
            'cmd': self.cmd,
            'output': result['stdout'],
            'stderr': result['stderr'],
            'returncode': result['returncode'],
            'timestamp': timestamp,
        })
