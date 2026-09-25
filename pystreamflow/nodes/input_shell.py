import asyncio
from datetime import datetime, timezone

from ..core.exec_guard import command_execution_allowed
from ..core.node import BaseNode
from ..core.paths import data_dir, ensure_dir
from ..core.subprocess_exec import run_subprocess


class ShellInputNode(BaseNode):
    """Shell Input Node.

    Periodically runs a real shell command line (``config['command']``)
    via ``/bin/sh -c`` and emits its actual stdout/stderr/returncode -
    not a canned sample. Unlike ``ProcessInputNode``, this goes through a
    real shell, so pipes, redirection, globs and other shell syntax work.

    TRUST BOUNDARY: same reasoning as ``ProcessInputNode`` (workflow
    config isn't necessarily trusted - see that node's docstring for the
    full disclosure) but broader in effect, since a whole shell is
    available to the configured command rather than just direct process
    exec. Gated by ``PSF_ALLOW_SHELL_NODES``
    (``pystreamflow/core/exec_guard.py``); set it to ``0`` to
    hard-disable this node's execution path.

    Config:
      command: shell command line, e.g. ``'date'`` (default)
      interval: seconds between runs, ``0`` = run once (default ``2.0``)
      timeout: seconds allowed per run (default ``30.0``)
      cwd: working directory for the command (default: ``data_dir()`` -
        ``PSF_DATA_DIR``, ``/app/data`` under the packaged
        docker-compose.yml - rather than this process's own container-
        internal cwd, so a command like ``'ls *.csv'`` reads/writes
        somewhere the operator can actually see by default; created if
        it doesn't exist yet)
    """

    async def init(self):
        self.command = self.config.get('command', 'date')
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
            self.emit('out', {'command': self.command, 'error': msg, 'timestamp': timestamp})
            return

        result = await run_subprocess(self.command, shell=True, cwd=self.cwd, timeout=self.timeout)
        if 'error' in result:
            self._last_error = result['error']
            self.emit('out', {'command': self.command, 'error': result['error'], 'timestamp': timestamp})
            return

        self.emit('out', {
            'command': self.command,
            'output': result['stdout'],
            'stderr': result['stderr'],
            'returncode': result['returncode'],
            'timestamp': timestamp,
        })
