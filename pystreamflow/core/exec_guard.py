"""Shared trust-boundary gates for every node that lets workflow config
drive real code/command execution or arbitrary outbound network
connections.

This centralizes the pattern first established in
``pystreamflow/nodes/scripted_output.py`` (Phase 6 hardening audit,
action-plan item 2.9): config values in this project can come from an
unauthenticated HTTP API call (``pystreamflow/api/server.py`` has no
authentication at all) or from a saved workflow file just as easily as
from a trusted operator, so any node whose config can make this process
run code, run a shell command, or open a connection to an
attacker-chosen destination is a deliberate, documented trust boundary -
not a bug to quietly work around. Each gate below defaults to *allowed*
(flipping the default to "off" would silently break every workflow
already using the node the moment this shipped, which is a worse
surprise than the trust boundary itself) and can be hard-disabled with
an environment variable for anyone deploying this project somewhere it
might be reachable by users they don't fully trust.
"""
import os


def _env_flag_allowed(var_name: str) -> bool:
    return os.environ.get(var_name, '1').strip().lower() not in ('0', 'false', 'no', 'off')


def script_execution_allowed() -> bool:
    """Gate for nodes that ``exec()`` arbitrary Python source pulled from
    config against streamed data: ``ScriptedOutputNode``, ``ScriptNode``,
    ``PythonScriptInputNode``. Controlled by ``PSF_ALLOW_SCRIPT_NODES``.
    """
    return _env_flag_allowed('PSF_ALLOW_SCRIPT_NODES')


def command_execution_allowed() -> bool:
    """Gate for nodes that spawn a real OS subprocess (direct exec or a
    real shell) from config: ``ProcessInputNode``, ``ShellInputNode``,
    ``ProcessOutputNode``. Controlled by ``PSF_ALLOW_SHELL_NODES``.
    """
    return _env_flag_allowed('PSF_ALLOW_SHELL_NODES')


def network_send_allowed() -> bool:
    """Gate for nodes that open an outbound network connection to a
    config-supplied host/port: ``SocketOutputNode``. This is an
    SSRF-shaped risk (a workflow can be pointed at any network-reachable
    destination from wherever this process runs, including internal-only
    services) rather than a code-execution one, but the same "config
    isn't necessarily trusted" reasoning applies, so it gets the same
    kind of gate. Controlled by ``PSF_ALLOW_SOCKET_NODES``.
    """
    return _env_flag_allowed('PSF_ALLOW_SOCKET_NODES')
