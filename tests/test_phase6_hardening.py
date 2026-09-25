"""
Regression tests for the Phase 6 hardening pass (see
claude/evaluation_and_action_plan.md's Phase 6 completion report).

Three real bug categories were found and fixed this phase, each verified
here against real node instances (never by inspection alone):

1. Self-clobbering output wiring. Seven "self-contained" node types
   (SocketInputNode, MQTTInputNode, LMStudioNode, WebInputNode,
   UserInputNode, ScriptedOutputNode, ScriptInputNode) used to
   unconditionally call add_output('out', <their own internal pipe>) in
   init() - which runs during start(), *after* Engine._wire_edges() has
   already wired the node's real downstream output pipe (see
   BaseNode.add_output_if_unwired()'s docstring for the full mechanism).
   That silently overwrote the real wiring every time, so none of these
   seven node types could ever deliver output to anything in a real
   graph - exactly the same bug class as SubgraphNode, fixed in Phase 5.
   Every test below that pre-wires an output pipe *before* calling
   start() (mirroring exactly what Engine._wire_edges() does) and then
   asserts delivery through *that* pipe is a direct regression test for
   this fix - it fails against the pre-fix code because the item would
   only ever reach the node's own internal pipe instead.

2. MQTTInputNode's _on_message() scheduled delivery via
   `asyncio.create_task()` unconditionally, but paho invokes it from a
   plain (non-asyncio) background thread in real usage - which has no
   running event loop, so create_task() raised RuntimeError, and that
   RuntimeError was itself swallowed by a blind `except Exception: pass`.
   Net effect: MQTTInputNode looked healthy but silently dropped every
   real message. Fixed by detecting the calling thread and routing
   through run_coroutine_threadsafe() when there's no running loop.

3. ScriptInputNode could leak subprocesses. Cancelling the node's task
   mid-run (the normal way stop() works) left the child process running
   forever with no cleanup at all. Separately, even the pre-existing
   timeout-handling code only killed the *direct* child, leaving any
   grandchild processes (e.g. a real command invoked by a shell-script
   wrapper) running as orphans - and, worse, a lingering grandchild that
   inherited the stdout/stderr pipe file descriptors could make a
   subsequent proc.communicate() call hang forever waiting for pipe EOF
   that would never arrive. Fixed by launching with
   start_new_session=True and killing the whole process group on both
   paths.

Plus a fourth, unrelated real bug found in the same file while writing
its coverage tests back in Phase 5 and fixed properly here: dead code in
ScriptedOutputNode meant a `result = ...`-style script always silently
produced `output: None` (see test_scripted_output_result_style_now_works
below) - and, since exec() with an inline script is a genuine, deliberate
trust boundary, a documented opt-out env var was added alongside the fix.
"""
import asyncio
import os
import threading

import httpx
import pytest

from pystreamflow.core.node import BaseNode
from pystreamflow.core.stream import Pipe

# ---------- BaseNode.add_output_if_unwired() ----------

class _Minimal(BaseNode):
    async def init(self):
        pass
    async def process(self):
        while self._running:
            await asyncio.sleep(1)


def test_add_output_if_unwired_creates_when_absent():
    n = _Minimal('n', {})
    p = Pipe()
    result = n.add_output_if_unwired('out', p)
    assert result is p
    # Phase 1 fan-out change: self.outputs[name] is now a list of every
    # live consumer pipe (see core/node.py's BaseNode.outputs docstring),
    # not a single Pipe - this port has exactly one consumer here. Phase 2
    # added a per-consumer delivery ``kind`` alongside each pipe, so each
    # entry is now a ``(Pipe, kind)`` tuple; this internal placeholder pipe
    # is always ``"data"``.
    assert n.outputs['out'] == [(p, 'data')]


def test_add_output_if_unwired_preserves_existing_wiring():
    n = _Minimal('n', {})
    real_pipe = Pipe()
    n.add_output('out', real_pipe)  # simulates Engine._wire_edges()
    internal_pipe = Pipe()
    result = n.add_output_if_unwired('out', internal_pipe)
    assert result is real_pipe
    # Phase 1 fan-out change: add_output_if_unwired() must not append a
    # second consumer alongside the real wiring already in place - the
    # port should still have exactly the one real pipe, not [real_pipe,
    # internal_pipe]. Phase 2's (Pipe, kind) tuple shape applies here too.
    assert n.outputs['out'] == [(real_pipe, 'data')]


async def test_add_output_appends_instead_of_overwriting():
    # Phase 1 fan-out fix (see claude/design_unified_wire_kinds_plan.md):
    # a second wire drawn from the same output port name used to silently
    # replace the first consumer's pipe - self.outputs[name] was a plain
    # `= pipe` overwrite. Two consumers wired to the same output port must
    # now both receive every emitted item.
    n = _Minimal('n', {})
    first, second = Pipe(), Pipe()
    n.add_output('out', first)
    n.add_output('out', second)
    # Phase 2's (Pipe, kind) tuple shape - both wired here as plain "data".
    assert n.outputs['out'] == [(first, 'data'), (second, 'data')]
    n.emit('out', 'hello')
    assert await asyncio.wait_for(first.get(), timeout=2.0) == 'hello'
    assert await asyncio.wait_for(second.get(), timeout=2.0) == 'hello'


def test_remove_output_removes_only_the_named_pipe():
    # Counterpart to the fan-out fix above: disconnecting one of several
    # consumers on the same output port must leave the others untouched -
    # the old disconnect path's `del src.outputs[source_port]` would have
    # killed every consumer, not just the one edge being removed.
    n = _Minimal('n', {})
    first, second = Pipe(), Pipe()
    n.add_output('out', first)
    n.add_output('out', second)
    assert n.remove_output('out', first) is True
    # Phase 2's (Pipe, kind) tuple shape.
    assert n.outputs['out'] == [(second, 'data')]
    # Removing the last remaining consumer drops the port entirely, same
    # as the old "no one connected here" state.
    assert n.remove_output('out', second) is True
    assert 'out' not in n.outputs
    # Removing again (already gone) or an unknown pipe/port is a no-op,
    # not an error.
    assert n.remove_output('out', first) is False
    assert n.remove_output('never-wired', Pipe()) is False


# ---------- ScriptedOutputNode: wiring, dead-code fix, trust-boundary gate ----------

async def test_scripted_output_delivers_to_externally_wired_pipe():
    from pystreamflow.nodes.scripted_output import ScriptedOutputNode
    n = ScriptedOutputNode('n', {'script': 'return data * 2'})
    in_pipe, real_out = Pipe(), Pipe()
    n.add_input('in', in_pipe)
    n.add_output('out', real_out)  # pre-wired, exactly like Engine._wire_edges()
    await n.start()
    try:
        await in_pipe.put(21)
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'input': 21, 'output': 42}
    finally:
        await n.stop()


async def test_scripted_output_result_style_now_works():
    from pystreamflow.nodes.scripted_output import ScriptedOutputNode
    n = ScriptedOutputNode('n', {'script': 'result = data * 2'})
    in_pipe = Pipe()
    n.add_input('in', in_pipe)
    await n.start()
    try:
        await in_pipe.put(21)
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item == {'input': 21, 'output': 42}
    finally:
        await n.stop()


async def test_scripted_output_return_style_still_works():
    from pystreamflow.nodes.scripted_output import ScriptedOutputNode
    n = ScriptedOutputNode('n', {'script': 'return data * 2'})
    in_pipe = Pipe()
    n.add_input('in', in_pipe)
    await n.start()
    try:
        await in_pipe.put(21)
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item == {'input': 21, 'output': 42}
    finally:
        await n.stop()


async def test_scripted_output_execution_can_be_disabled(monkeypatch):
    from pystreamflow.nodes.scripted_output import ScriptedOutputNode
    monkeypatch.setenv('PSF_ALLOW_SCRIPT_NODES', '0')
    n = ScriptedOutputNode('n', {'script': 'result = data * 2'})
    in_pipe = Pipe()
    n.add_input('in', in_pipe)
    await n.start()
    try:
        await in_pipe.put(21)
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item['input'] == 21
        assert 'error' in item and 'disabled' in item['error']
        assert 'output' not in item
    finally:
        await n.stop()


# ---------- The other six self-wiring node types: wiring fix only ----------

async def test_socket_input_delivers_to_externally_wired_pipe():
    from pystreamflow.nodes.input_socket import SocketInputNode
    n = SocketInputNode('n', {'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await asyncio.sleep(0.05)
        n.emit('out', {'probe': 1})
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'probe': 1}
    finally:
        await n.stop()


async def test_lmstudio_delivers_to_externally_wired_pipe(monkeypatch):
    import pystreamflow.nodes.llm_lmstudio as llm_module
    from pystreamflow.nodes.llm_lmstudio import LMStudioNode

    def handler(request):
        return httpx.Response(200, json={'choices': [{'message': {'content': 'hi'}}]})

    original_client = llm_module.httpx.AsyncClient

    def patched_client(*args, **kwargs):
        kwargs['transport'] = httpx.MockTransport(handler)
        return original_client(*args, **kwargs)

    monkeypatch.setattr(llm_module.httpx, 'AsyncClient', patched_client)

    n = LMStudioNode('n', {})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await asyncio.sleep(0.05)
        await n.inputs['in'].put('hello')
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {
            'prompt': 'hello', 'completion': 'hi', 'results': 'hi',
            'reasoning': None, 'usage': {},
        }
    finally:
        await n.stop()


async def test_web_input_delivers_to_externally_wired_pipe():
    # WebInputNode.process() starts the shared core.web_server app/uvicorn
    # instance (via start_server()) - module-global state shared with
    # every other WebInputNode/UserInputNode/WebOutputNode instance, by
    # design, so node.stop() deliberately does not tear it down (doing so
    # would break any other input node still relying on the same shared
    # server). This test started it, so this test is responsible for
    # stopping it again afterward - otherwise it leaks into whichever test
    # runs next in the same session (see
    # test_web_server_more_coverage.py's test_start_and_stop_server_lifecycle,
    # which asserts web_server._running starts False).
    from pystreamflow.core import web_server
    from pystreamflow.nodes.input_web import WebInputNode
    n = WebInputNode('n', {'path': '/phase6_test_in', 'port': 0})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await asyncio.sleep(0.05)
        n.emit('out', {'v': 1})
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == {'v': 1}
    finally:
        await n.stop()
        await web_server.stop_server()


async def test_user_input_node_delivers_to_externally_wired_pipe():
    from pystreamflow.nodes.user_input_node import UserInputNode
    n = UserInputNode('n', {'path': '/phase6_user_input'})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await asyncio.sleep(0.05)
        n.emit('out', 'abc')
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item == 'abc'
    finally:
        await n.stop()


async def test_script_input_node_delivers_to_externally_wired_pipe(tmp_path):
    from pystreamflow.nodes.input_script import ScriptInputNode
    script = tmp_path / 'echo.sh'
    script.write_text('#!/bin/sh\necho hello\n')
    script.chmod(0o755)
    n = ScriptInputNode('n', {'script_path': str(script), 'interval': 0, 'emit_mode': 'full'})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        item = await asyncio.wait_for(real_out.get(), timeout=5.0)
        assert item['returncode'] == 0
        assert 'hello' in item['stdout']
    finally:
        await n.stop()


# ---------- MQTTInputNode: cross-thread delivery + wiring ----------

class _FakeMsg:
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload.encode()
        self.qos = 0
        self.retain = False


class _FakeMQTTClient:
    """Bare-minimum stand-in for paho.mqtt.client.Client so these tests
    don't spend their background thread trying (and failing) to reach a
    real broker - see tests/test_mqtt_coverage.py's own FakeMQTTClient,
    which this mirrors just enough for the two tests below."""
    def __init__(self, *args, **kwargs):
        self.on_connect = None
        self.on_message = None
    def username_pw_set(self, *a, **k):
        pass
    def connect(self, *a, **k):
        pass
    def loop_forever(self):
        import time
        while not getattr(self, '_disconnected', False):
            time.sleep(0.02)
    def subscribe(self, *a, **k):
        pass
    def disconnect(self):
        self._disconnected = True


@pytest.fixture
def fake_mqtt_client(monkeypatch):
    import pystreamflow.nodes.mqtt_input as mqtt_input_module
    monkeypatch.setattr(mqtt_input_module.mqtt, 'Client', _FakeMQTTClient)


async def test_mqtt_input_on_message_delivers_from_a_real_background_thread(fake_mqtt_client):
    """Regression test for the cross-thread asyncio.create_task() bug:
    paho really does call _on_message from a plain OS thread (see
    mqtt_input.py's process(), which runs client.loop_forever() inside a
    threading.Thread) - unlike the rest of this project's MQTT tests,
    which call _on_message directly from the event-loop thread for
    simplicity (see tests/test_mqtt_coverage.py's own docstring). This
    test specifically exercises the real cross-thread path the bug was
    in, by calling _on_message from an actual background thread rather
    than from the test's own coroutine."""
    from pystreamflow.nodes.mqtt_input import MQTTInputNode
    n = MQTTInputNode('n', {})
    await n.start()
    try:
        await asyncio.sleep(0.1)  # let process() run far enough to set self._loop
        assert n._loop is not None

        errors = []
        def call_from_thread():
            try:
                n._on_message(None, None, _FakeMsg('t/x', '{"a": 1}'))
            except Exception as e:  # noqa: BLE001 - deliberately broad: this
                # is exactly what the pre-fix RuntimeError from a bare
                # asyncio.create_task() call in the wrong thread looked
                # like, and asserting `not errors` below is the point of
                # the test, so any exception at all here is a regression.
                errors.append(e)
        t = threading.Thread(target=call_from_thread)
        t.start()
        t.join(timeout=2)

        assert not errors
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item == {'topic': 't/x', 'payload': {'a': 1}, 'qos': 0, 'retain': False}
    finally:
        await n.stop()


async def test_mqtt_input_delivers_to_externally_wired_pipe(fake_mqtt_client):
    from pystreamflow.nodes.mqtt_input import MQTTInputNode
    n = MQTTInputNode('n', {})
    real_out = Pipe()
    n.add_output('out', real_out)
    await n.start()
    try:
        await asyncio.sleep(0.05)
        n._on_message(None, None, _FakeMsg('t/y', 'plain text'))
        item = await asyncio.wait_for(real_out.get(), timeout=2.0)
        assert item['payload'] == 'plain text'
    finally:
        await n.stop()


# ---------- ScriptInputNode: process-group cleanup ----------

@pytest.fixture
def sleep_script(tmp_path):
    script = tmp_path / 'sleep_script.sh'
    script.write_text('#!/bin/sh\nsleep 30\n')
    script.chmod(0o755)
    return str(script)


async def test_script_input_node_stop_kills_process_group_not_just_direct_child(sleep_script):
    """Regression test for the subprocess-leak fix: cancelling the node's
    task (what stop() does) used to leave the child process running
    forever with zero cleanup. Confirms the direct child is actually
    killed (returncode set) rather than merely abandoned, and that
    stop() itself returns promptly instead of hanging - see
    _kill_proc_tree()'s docstring for why an unconditional proc.kill() +
    proc.wait() alone isn't quite enough for a shell-wrapper script."""
    from pystreamflow.nodes.input_script import ScriptInputNode
    n = ScriptInputNode('n', {'script_path': sleep_script, 'interval': 0, 'timeout': 60})
    await n.start()
    try:
        for _ in range(50):
            await asyncio.sleep(0.1)
            if n._current_proc is not None:
                break
        proc = n._current_proc
        assert proc is not None, 'subprocess never started'
        pgid = os.getpgid(proc.pid)
    finally:
        await asyncio.wait_for(n.stop(), timeout=5.0)

    assert proc.returncode is not None, 'direct child was never reaped'
    # The whole process group was sent SIGKILL; confirm nothing in it is
    # still consuming CPU by polling briefly rather than requiring
    # instantaneous removal from the process table (a killed grandchild
    # can sit as a zombie for a moment until its reparented owner reaps
    # it - normal OS behavior, not something this node controls).
    for _ in range(20):
        try:
            os.killpg(pgid, 0)
        except ProcessLookupError:
            break
        await asyncio.sleep(0.1)
    else:
        pytest.fail('process group still alive 2s after stop()')


async def test_script_input_node_timeout_also_kills_process_group(sleep_script):
    from pystreamflow.nodes.input_script import ScriptInputNode
    n = ScriptInputNode('n', {'script_path': sleep_script, 'interval': 0, 'timeout': 0.3})
    await n.start()
    try:
        await asyncio.sleep(1.0)
        assert n._last_error and 'timed out' in n._last_error
    finally:
        await n.stop()
