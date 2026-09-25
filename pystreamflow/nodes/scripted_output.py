import asyncio

from ..core.exec_guard import script_execution_allowed as _script_execution_allowed
from ..core.node import BaseNode
from ..core.stream import Pipe

# _script_execution_allowed is re-exported here (rather than only living in
# core/exec_guard.py) for backwards compatibility with anything importing it
# from this module, and because this is still the class whose docstring
# carries the full trust-boundary disclosure that every other script/command/
# socket-executing node now points back to. The gate itself - and the
# PSF_ALLOW_SCRIPT_NODES env var it reads - now lives in core/exec_guard.py
# so ScriptNode and PythonScriptInputNode (pystreamflow/nodes/modifier_script.py,
# input_python.py) can share the exact same check instead of duplicating it.


class ScriptedOutputNode(BaseNode):
    """Runs a user-supplied Python snippet (``config['script']``) against
    every item this node receives, via exec() with the full privileges of
    this process - deliberately: it's meant as a general escape hatch for
    transforms nothing else in the node library covers.

    TRUST BOUNDARY (Phase 6 hardening audit, action-plan item 2.9):
    ``config['script']`` is executed as real Python code, completely
    unsandboxed - it can read/write any file this process can, open
    sockets, import anything, etc. Anything that can create or reconfigure
    this node can therefore run arbitrary code on the host running
    pystreamflow: the HTTP API (`api/server.py`) has no authentication at
    all, the MCP server's `MCP_API_KEY` is optional and off by default,
    and a saved workflow file is just as capable of carrying a malicious
    script as any other config value. This is inherent to the feature -
    not a bug to silently work around by neutering it - but it needs to be
    visible to anyone deciding how to deploy this project. If you expose
    either server beyond a fully-trusted network, put an authenticating
    proxy in front of it, or set the environment variable
    ``PSF_ALLOW_SCRIPT_NODES=0`` to hard-disable this node's execution
    path (it emits a clear error item instead of running the script,
    rather than failing silently).
    """

    async def init(self):
        self.script = self.config.get('script', 'return data')
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start(),
        # exactly like the SubgraphNode bug fixed in Phase 5. Verified: a
        # ScriptedOutputNode wired into a real graph (add_output() called
        # externally, as the Engine does, before start()) could never
        # deliver output anywhere - every item silently went into this
        # node's own internal, nothing-reads-it pipe instead.
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self._compile_errors = []

    async def process(self):
        pipe = self.inputs.get('in')
        if not pipe:
            # wait for connection
            while self._running and 'in' not in self.inputs:
                await asyncio.sleep(0.5)
            pipe = self.inputs.get('in')
        if not pipe:
            return

        while self._running:
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.01)
                continue

            if not _script_execution_allowed():
                out_item = {'input': item, 'error': 'script execution disabled (PSF_ALLOW_SCRIPT_NODES=0)'}
                self._last_error = out_item['error']
                self.emit('out', out_item)
                continue

            try:
                local_ns = {'data': item, 'emit': lambda x: None}
                # Bug fix (Phase 6): this used to always wrap the script as
                # a function body and call it, only falling back to a
                # direct top-level exec() "if not callable(fn)" - but the
                # wrapped exec() always succeeds in defining __script__ as
                # a real function whenever the script is syntactically
                # valid at all, so that fallback branch could never
                # actually run. The practical effect: a script written in
                # the (also reasonable, and apparently intended given the
                # dead code that assumed it worked) "assign to `result`"
                # style - e.g. `result = data * 2`, mirroring how
                # TemplateNode/JSONModifyNode etc. read a plain config
                # value rather than requiring a `return` - silently always
                # produced `output: None`, since that assignment just
                # became a local variable inside the wrapped function and
                # was discarded when it returned. Verified with a
                # standalone script before this fix.
                #
                # Fixed by trying the script as plain top-level statements
                # first (this is what makes `result = ...` scripts work -
                # exec() with the same dict as both globals and locals
                # updates that dict directly for top-level assignments),
                # and only falling back to the function-wrapping approach
                # on a SyntaxError - which is exactly what a `return ...`
                # script produces at top level (`return` outside a
                # function is a SyntaxError), so both styles now work:
                # test_new_nodes.py's existing `'return data * 2'` case via
                # the fallback, and the previously-broken `result = ...`
                # style via the new primary path.
                try:
                    exec(self.script, {}, local_ns)
                    out_item = {'input': item, 'output': local_ns.get('result')}
                except SyntaxError:
                    code = "def __script__(data):\n"
                    for line in self.script.splitlines():
                        code += f"    {line}\n"
                    exec(code, {}, local_ns)
                    fn = local_ns.get('__script__')
                    result = fn(item) if callable(fn) else None
                    out_item = {'input': item, 'output': result}
            except Exception as e:
                out_item = {'input': item, 'error': str(e)}
                self._last_error = str(e)

            self.emit('out', out_item)
