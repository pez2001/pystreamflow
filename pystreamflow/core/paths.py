"""Shared default-directory resolution for nodes that touch the real
filesystem: ``FileOutputNode``'s file, ``LogOutputNode``'s real log
file, and the working directory ``ProcessInputNode``/``ShellInputNode``/
``ProcessOutputNode`` run their subprocesses in.

Before this, each of those nodes either hardcoded a path with no
relationship to anything an operator could actually reach (the old
``FileOutputNode`` default was ``/tmp/out.txt`` - inside a Docker
container that's an ephemeral, never-mounted path that vanishes with
the container and was never visible on the host at all) or defaulted to
this *process's own* current working directory (``ProcessInputNode``/
``ShellInputNode``'s ``cwd`` config, when left unset) - which under the
packaged ``Dockerfile`` is ``/app``, not any of the mounted volumes
either.

Centralizing the default here - one place, one set of environment
variables - means the packaged ``docker-compose.yml``/
``docker-compose.prod.yml`` only need to set three env vars and mount
three matching host directories, and every node that writes/reads real
files or spawns subprocesses lands somewhere the operator can actually
see, by default, without having to know and override each node's own
config with an absolute in-container path by hand. A workflow can
always still override any of this per-node (``path``/``cwd`` in that
node's own config) exactly as before - these are only the fallback used
when the operator hasn't set one.
"""
import os


def logs_dir() -> str:
    """Where ``LogOutputNode``'s real log file is written. Defaults to a
    relative ``./logs`` (created on first use if missing) so this also
    works untouched outside Docker; the packaged compose files override
    ``PSF_LOGS_DIR`` to the ``/app/logs`` mount point.
    """
    return os.environ.get('PSF_LOGS_DIR', 'logs')


def files_dir() -> str:
    """Where ``FileOutputNode`` writes by default. See ``logs_dir()``."""
    return os.environ.get('PSF_FILES_DIR', 'files')


def data_dir() -> str:
    """Default working directory for ``ProcessInputNode``/
    ``ShellInputNode``/``ProcessOutputNode``'s subprocesses (used only
    when a node's own ``cwd`` config is left unset), so a workflow step
    like ``'cat report.csv'`` or ``'python analyze.py > result.json'``
    reads/writes somewhere the operator can see by default, via the
    packaged ``/app/data`` mount, rather than this process's own
    container-internal working directory.
    """
    return os.environ.get('PSF_DATA_DIR', 'data')


def ensure_dir(path: str) -> None:
    """``mkdir -p path`` itself - unlike ``ensure_parent_dir()`` below,
    which ensures the *parent* of a file path. Used for the cwd-style
    directories ``ProcessInputNode``/``ShellInputNode``/
    ``ProcessOutputNode`` default their subprocesses into, so a freshly
    checked-out project (or a freshly created, still-empty Docker
    volume) doesn't fail the very first run with a "no such directory"
    error just because nothing has used it yet.
    """
    if path:
        os.makedirs(path, exist_ok=True)


def ensure_parent_dir(path: str) -> None:
    """``mkdir -p`` the directory a file at ``path`` will be written
    into, if it doesn't already exist - so the *first* write against a
    freshly-mounted, still-empty volume doesn't fail with
    ``FileNotFoundError`` just because nothing has ever written there
    yet. A no-op (not an error) if ``path`` has no directory component.
    """
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
