"""Shared, hardened real-subprocess execution helper.

This is the same process-group timeout/kill hardening
``pystreamflow/nodes/input_script.py``'s ``ScriptInputNode`` already uses
(see that module's ``_run_once``/``_kill_proc_tree`` docstrings for the
original bug reports this avoids repeating: a hung child left running
forever because only the direct child was killed, or because a
cancelled asyncio task skipped cleanup entirely), factored out here so
every other node that spawns a real subprocess from streamed/config data
(``ProcessInputNode``, ``ShellInputNode``, ``ProcessOutputNode``) gets
the identical protection instead of re-implementing (and potentially
under-implementing) it independently.
"""
import asyncio
import os
import signal


async def kill_proc_tree(proc: asyncio.subprocess.Process):
    """Force-kill the whole process group started for ``proc`` (it must
    have been launched with ``start_new_session=True``), not just the
    immediate child, then wait for it to be reaped. See
    ``ScriptInputNode._kill_proc_tree`` for the full rationale - reused
    verbatim here rather than duplicated.
    """
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        # Already dead, or we're not the group leader's owner (e.g. a
        # restricted sandbox) - fall back to killing just the direct
        # child rather than raising out of a cleanup path.
        try:
            proc.kill()
        except ProcessLookupError:
            pass
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        pass


async def run_subprocess(cmd, *, shell: bool = False, cwd=None, env=None,
                          timeout: float = 30.0, input_data=None) -> dict:
    """Run a real subprocess to completion and return its result.

    ``cmd`` is a list of argv when ``shell`` is False (exec'd directly,
    no shell interpretation - shell metacharacters are inert), or a
    single command-line string passed to ``/bin/sh -c`` when ``shell`` is
    True. ``input_data``, if given, is written to the child's stdin
    (``str`` is UTF-8 encoded first) and stdin is closed afterward so a
    child reading to EOF doesn't hang.

    Returns ``{'returncode', 'stdout', 'stderr'}`` on completion, or
    ``{'error': <message>}`` if the command couldn't be started or timed
    out. Never raises for an ordinary failure (bad path, non-zero exit,
    timeout) - callers decide how to represent that as an emitted item;
    it only lets ``asyncio.CancelledError`` propagate (after killing the
    child), since that means this node itself is being stopped.
    """
    stdin_bytes = None
    if input_data is not None:
        stdin_bytes = input_data.encode('utf-8') if isinstance(input_data, str) else bytes(input_data)

    try:
        if shell:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                stdin=asyncio.subprocess.PIPE if stdin_bytes is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env,
                start_new_session=True,
            )
        else:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE if stdin_bytes is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                env=env,
                start_new_session=True,
            )
    except (FileNotFoundError, PermissionError, NotADirectoryError, OSError) as e:
        return {'error': str(e)}

    try:
        try:
            stdout_data, stderr_data = await asyncio.wait_for(
                proc.communicate(stdin_bytes), timeout=timeout
            )
        except asyncio.TimeoutError:
            await kill_proc_tree(proc)
            return {'error': f'command timed out after {timeout}s'}
        except asyncio.CancelledError:
            await kill_proc_tree(proc)
            raise
    except Exception as e:
        return {'error': str(e)}

    return {
        'returncode': proc.returncode,
        'stdout': stdout_data.decode('utf-8', errors='replace'),
        'stderr': stderr_data.decode('utf-8', errors='replace'),
    }
