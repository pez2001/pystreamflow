import asyncio
import os
import shlex
import signal
from datetime import datetime
from pathlib import Path

from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node


class ScriptInputNode(BaseNode):
    """
    Script Input Node
    Executes a script file periodically and emits stdout/stderr lines.
    Supports Python or any executable script.
    Config options:
      script_path: path to script file, required
      interpreter: interpreter command, e.g. 'python3' for python scripts, '' for direct exec
      args: list of args or space separated string
      interval: seconds between executions, 0 for run once
      working_dir: cwd for execution
      timeout: seconds per run
      emit_mode: 'lines' | 'full' | 'json'
      env: dict of environment variables to set
    """
    async def init(self):
        self.script_path = self.config.get('script_path')
        if not self.script_path:
            raise ValueError("script_path is required for ScriptInputNode")
        self.interpreter = self.config.get('interpreter', '')
        self.args = self.config.get('args', [])
        if isinstance(self.args, str):
            self.args = shlex.split(self.args)
        self.interval = float(self.config.get('interval', 5.0))
        self.working_dir = self.config.get('working_dir')
        self.timeout = float(self.config.get('timeout', 30.0))
        self.emit_mode = self.config.get('emit_mode', 'lines')
        self.env = {**os.environ, **self.config.get('env', {})}
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self._running_flag = False
        register_node(self)
        self._last_run = None
        self._current_proc = None

    async def process(self):
        self._running_flag = True
        while self._running:
            try:
                await self._run_once()
            except Exception as e:
                self._last_error = str(e)
                self.emit('out', {'error': self._last_error, 'timestamp': datetime.utcnow().isoformat()})
            # Wait for next interval
            if self.interval > 0:
                await asyncio.sleep(self.interval)
            else:
                break

    async def _run_once(self):
        script = Path(self.script_path)
        if not script.exists():
            raise FileNotFoundError(f"Script not found: {self.script_path}")
        cmd = []
        if self.interpreter:
            cmd.append(self.interpreter)
        cmd.append(str(script))
        cmd.extend(self.args)
        cwd = self.working_dir or str(script.parent)
        # Run subprocess with asyncio
        #
        # Phase 6 hardening fix: start_new_session=True puts the child in
        # its own process group (with the child itself as group leader),
        # so a forced shutdown below can target the *whole* tree via
        # os.killpg() rather than only the direct child - see
        # _kill_proc_tree()'s docstring for why that distinction matters.
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd,
            env=self.env,
            start_new_session=True,
        )
        # Track the in-flight subprocess so a forced shutdown (timeout, or
        # this node being stopped) can reap it. Previously only the
        # explicit `except asyncio.TimeoutError:` branch below ever killed
        # a hung child process - if the node's own asyncio task was
        # cancelled instead (the normal way stop() ends a running node -
        # see BaseNode.stop() -> _cancel_task()), proc.communicate() raised
        # CancelledError, which propagated straight past this method with
        # no cleanup at all, leaving the child process running as an
        # orphan forever. Verified with a standalone script: starting a
        # ScriptInputNode against a `sleep 30`-style script and calling
        # node.stop() while it was mid-run left the sleep process alive in
        # the process table (proc.returncode stayed None) before this fix.
        self._current_proc = proc
        try:
            try:
                stdout_data, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)
            except asyncio.TimeoutError:
                await self._kill_proc_tree(proc)
                raise TimeoutError(f"Script timed out after {self.timeout}s")
            except asyncio.CancelledError:
                await self._kill_proc_tree(proc)
                raise
        finally:
            self._current_proc = None

        returncode = proc.returncode
        timestamp = datetime.utcnow().isoformat()
        out_text = stdout_data.decode('utf-8', errors='replace')
        err_text = stderr_data.decode('utf-8', errors='replace')
        self._last_run = {
            'returncode': returncode,
            'timestamp': timestamp,
            'stdout': out_text,
            'stderr': err_text,
        }
        if self.emit_mode == 'lines':
            for line in out_text.splitlines():
                self.emit('out', {
                    'line': line,
                    'timestamp': timestamp,
                    'returncode': returncode,
                    'script': str(script)
                })
            if err_text:
                for line in err_text.splitlines():
                    self.emit('out', {
                        'line': line,
                        'stream': 'stderr',
                        'timestamp': timestamp,
                        'returncode': returncode,
                        'script': str(script)
                    })
        elif self.emit_mode == 'full':
            self.emit('out', {
                'stdout': out_text,
                'stderr': err_text,
                'returncode': returncode,
                'timestamp': timestamp,
                'script': str(script)
            })
        elif self.emit_mode == 'json':
            try:
                import json
                payload = json.loads(out_text or '{}')
                self.emit('out', {
                    'payload': payload,
                    'stderr': err_text,
                    'returncode': returncode,
                    'timestamp': timestamp,
                    'script': str(script)
                })
            except Exception:
                self.emit('out', {
                    'stdout': out_text,
                    'stderr': err_text,
                    'returncode': returncode,
                    'timestamp': timestamp,
                    'script': str(script)
                })
        else:
            self.emit('out', {
                'stdout': out_text,
                'stderr': err_text,
                'returncode': returncode,
                'timestamp': timestamp,
                'script': str(script)
            })

    async def _kill_proc_tree(self, proc):
        """Force-kill the whole process group started for a run, not just
        the immediate child, then wait for the direct child to be reaped -
        using proc.wait() rather than proc.communicate() so this can't
        itself hang on pipe state (see below).

        Two related bugs this fixes together: a script that's a shell
        wrapper (or that forks its own children) leaves those grandchild
        processes running as orphans if only the direct child is killed -
        which was already true of the pre-existing timeout-handling
        `proc.kill()` call this replaces, not something this fix
        introduced. Worse, a grandchild that inherited the stdout/stderr
        pipe file descriptors keeps them open even after the direct child
        dies, so a subsequent proc.communicate() call can hang forever
        waiting for pipe EOF that will now never arrive. Verified both
        with a `#!/bin/bash\nsleep 30` script: killing only the direct
        bash process left `sleep` running indefinitely, and a
        communicate() call issued afterward hung until forcibly
        interrupted. Because _run_once() launches with
        start_new_session=True, the whole tree shares one process group
        this can target with os.killpg() - reliably ending every
        descendant, not just the one this code directly spawned.
        """
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            # Already dead, or we're not the group leader's owner for some
            # reason (e.g. a restricted sandbox) - fall back to killing
            # just the direct child rather than raising out of a cleanup
            # path.
            proc.kill()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            # Nothing more we can safely do here without risking hanging
            # the caller's own shutdown indefinitely; the process group
            # was already sent SIGKILL above.
            pass

    async def stop(self):
        self._running_flag = False
        await super().stop()
