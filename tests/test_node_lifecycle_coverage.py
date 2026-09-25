"""
Coverage for pystreamflow/core/node.py's BaseNode - previously 74%
covered. Exercises the retry-on-error loop in _run_loop(), the
pause/resume/no-op edge cases, the non-auto-start add_input() path, the
control-message listener (including its error-breaks-the-listener path),
handle_control()'s branches, emit()'s failure bookkeeping, and the
_last_items ring-buffer truncation in both emit() and set_attribute().
"""
import asyncio

from pystreamflow.core.node import BaseNode
from pystreamflow.core.stream import Pipe


class _MinimalNode(BaseNode):
    async def process(self):
        while self._running:
            await asyncio.sleep(0)


# ---------- _run_loop retry behavior ----------

class _FlakyNode(BaseNode):
    """Fails process() a configurable number of times before succeeding."""
    def __init__(self, *args, fail_times=1, **kwargs):
        super().__init__(*args, **kwargs)
        self.fail_times = fail_times
        self.attempts = 0
        self.init_calls = 0

    async def init(self):
        self.init_calls += 1

    async def process(self):
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise RuntimeError(f'boom {self.attempts}')
        while self._running:
            await asyncio.sleep(0)


async def test_run_loop_retries_after_failure_then_succeeds():
    n = _FlakyNode('n', {'retries': 2, 'retry_delay': 0.01}, fail_times=1)
    await n.start()
    try:
        await asyncio.sleep(0.2)
        assert n.attempts == 2
        assert n._error_count == 1
        assert 'boom 1' in n._last_error
        assert n.init_calls == 2  # initial + one re-init before the retry
        assert n._health == 'healthy' or n._running
    finally:
        await n.stop()


async def test_run_loop_gives_up_after_exhausting_retries():
    n = _FlakyNode('n', {'retries': 1, 'retry_delay': 0.01}, fail_times=5)
    await n.start()
    try:
        await asyncio.sleep(0.2)
        assert n._health == 'error'
        assert n.attempts == 2  # initial attempt + exactly one retry
    finally:
        await n.stop()


async def test_run_loop_logs_but_continues_when_reinit_also_fails():
    class _FlakyInitNode(_FlakyNode):
        async def init(self):
            self.init_calls += 1
            if self.init_calls == 2:
                raise ValueError('reinit failed too')

    n = _FlakyInitNode('n', {'retries': 2, 'retry_delay': 0.01}, fail_times=1)
    await n.start()
    try:
        await asyncio.sleep(0.2)
        # Despite the re-init raising, the retry loop must not crash - it
        # just logs and tries process() again anyway. The retried
        # process() call now succeeds and loops forever (per _MinimalNode/
        # _FlakyNode's shape), so _health is never updated past whatever
        # the retry attempt set it to.
        assert n.attempts == 2
        assert n._health == 'retrying_1'
    finally:
        await n.stop()


async def test_run_loop_reaches_idle_health_when_process_returns_early():
    class _OneShotNode(BaseNode):
        async def process(self):
            return  # returns immediately without looping

    n = _OneShotNode('n', {})
    await n.start()
    await asyncio.sleep(0.05)
    assert n._health == 'idle'
    await n.stop()


# ---------- pause/resume edge cases ----------

async def test_resume_on_non_paused_node_is_a_no_op():
    n = _MinimalNode('n', {})
    await n.start()
    try:
        await n.resume()  # not paused - should just return
        assert n._running
    finally:
        await n.stop()

async def test_pause_on_non_running_node_is_a_no_op():
    n = _MinimalNode('n', {})
    await n.pause()
    assert not n._paused


# ---------- add_input without auto_start ----------

async def test_add_input_without_auto_start_still_counts_stats():
    n = _MinimalNode('n', {'auto_start': False})
    p = Pipe()
    n.add_input('in', p)
    await p.put('hello')
    item = await n.inputs['in'].get()
    assert item == 'hello'
    assert n._items_in == 1
    assert n._bytes_in > 0
    # Reading from the wrapped pipe must NOT have auto-started the node,
    # since auto_start is off.
    assert not n._running

async def test_add_input_control_pipe_is_stored_not_wrapped():
    n = _MinimalNode('n', {})
    control_pipe = Pipe()
    n.add_input('control', control_pipe)
    # Fan-in fix: a node's control input is now a list of every wired
    # control source (see BaseNode.__init__'s comment on
    # self._control_pipes) rather than a single overwritable pipe, so a
    # second control edge doesn't silently steal control from the first.
    assert n._control_pipes == [control_pipe]
    assert 'control' not in n.inputs

async def test_add_input_control_appends_a_second_source_instead_of_replacing():
    n = _MinimalNode('n', {})
    first = Pipe()
    second = Pipe()
    n.add_input('control', first)
    n.add_input('control', second)
    assert n._control_pipes == [first, second]

async def test_two_control_sources_both_still_deliver():
    # The actual bug this fixes: with the old single-slot `_control_pipe`,
    # wiring a second control source onto the same node silently killed
    # the first one's ability to ever reach this node again. Both must
    # keep working now.
    n = _MinimalNode('n', {})
    first = Pipe()
    second = Pipe()
    n.add_input('control', first)
    n.add_input('control', second)
    await n.start()
    try:
        await first.put({'action': 'pause'})
        await asyncio.sleep(0.6)
        assert n._paused
        await second.put({'action': 'resume'})
        await asyncio.sleep(0.6)
        assert n._running and not n._paused
        # The first source must still be listened to after the second one
        # was wired - this is the actual regression the old code had.
        await first.put({'action': 'pause'})
        await asyncio.sleep(0.6)
        assert n._paused
    finally:
        await n.stop()

async def test_remove_control_input_removes_only_that_source():
    n = _MinimalNode('n', {})
    first = Pipe()
    second = Pipe()
    n.add_input('control', first)
    n.add_input('control', second)
    await n.start()
    try:
        removed = await n.remove_control_input(first)
        assert removed is True
        assert n._control_pipes == [second]
        # The removed source's listener must be gone, but the remaining
        # source must still work.
        await second.put({'action': 'pause'})
        await asyncio.sleep(0.6)
        assert n._paused
    finally:
        await n.stop()

async def test_remove_control_input_on_unwired_pipe_returns_false():
    n = _MinimalNode('n', {})
    unwired = Pipe()
    assert await n.remove_control_input(unwired) is False


# ---------- control listener ----------

async def test_control_listener_dispatches_stop():
    n = _MinimalNode('n', {})
    control_pipe = Pipe()
    n.add_input('control', control_pipe)
    await n.start()
    try:
        assert n._running
        await control_pipe.put({'action': 'stop'})
        await asyncio.sleep(0.7)
        assert not n._running
        # stop() intentionally cancels every control listener itself too
        # (a fully-stopped node is fully stopped) - so the *same* control
        # pipe can no longer restart it; that requires an external
        # start() call, which is exactly what the API/MCP "start" tools
        # do rather than going through a (now-dead) control channel.
        assert n._control_listeners == {}
    finally:
        await n.stop()

async def test_control_listener_can_be_restarted_by_an_external_start():
    n = _MinimalNode('n', {})
    control_pipe = Pipe()
    n.add_input('control', control_pipe)
    await n.start()
    await control_pipe.put({'action': 'stop'})
    await asyncio.sleep(0.7)
    assert not n._running
    # A fresh, externally-triggered start() re-arms the control listener.
    await n.start()
    try:
        assert n._running
        await control_pipe.put({'action': 'stop'})
        await asyncio.sleep(0.7)
        assert not n._running
    finally:
        await n.stop()

async def test_control_listener_dispatches_pause_resume():
    n = _MinimalNode('n', {})
    control_pipe = Pipe()
    n.add_input('control', control_pipe)
    await n.start()
    try:
        await control_pipe.put({'action': 'pause'})
        await asyncio.sleep(0.7)
        assert n._paused
        await control_pipe.put({'action': 'resume'})
        await asyncio.sleep(0.7)
        assert n._running and not n._paused
    finally:
        await n.stop()

async def test_control_listener_breaks_on_handler_exception():
    class _BadControlNode(_MinimalNode):
        async def handle_control(self, msg):
            raise RuntimeError('handler exploded')

    n = _BadControlNode('n', {})
    control_pipe = Pipe()
    n.add_input('control', control_pipe)
    await n.start()
    try:
        await control_pipe.put({'action': 'anything'})
        await asyncio.sleep(0.7)
        # The listener task should have exited (broken out of its loop)
        # rather than looping forever or crashing the whole node.
        task = n._control_listeners.get(id(control_pipe))
        assert task is None or task.done()
        assert n._running  # the main process() loop is unaffected
    finally:
        await n.stop()


# ---------- handle_control branches ----------

async def test_handle_control_ignores_non_str_non_dict_messages():
    n = _MinimalNode('n', {})
    await n.handle_control(12345)  # neither str nor dict - must just return
    assert not n._running

async def test_handle_control_start_is_a_no_op_when_already_running():
    n = _MinimalNode('n', {})
    await n.start()
    try:
        first_task = n._task
        await n.handle_control('start')
        assert n._task is first_task  # unchanged - start() no-ops when running
    finally:
        await n.stop()

async def test_handle_control_string_command():
    n = _MinimalNode('n', {})
    await n.start()
    await n.handle_control('stop')
    assert not n._running


# ---------- emit() failure bookkeeping ----------

async def test_emit_records_failure_when_downstream_put_raises():
    n = _MinimalNode('n', {})
    bad_pipe = Pipe()

    async def failing_put(item, timeout=None):
        raise RuntimeError('downstream exploded')

    bad_pipe.put = failing_put
    n.add_output('out', bad_pipe)
    n.emit('out', 'x')
    await asyncio.sleep(0.05)
    assert n._error_count == 1
    assert 'downstream exploded' in n._last_error

async def test_emit_with_unwired_port_is_a_no_op():
    n = _MinimalNode('n', {})
    n.emit('out', 'x')  # nothing wired - must not raise
    assert n._items_out == 1  # stats still recorded even with no pipe


# ---------- _last_items ring buffer truncation ----------

async def test_emit_truncates_last_items_to_max_last():
    n = _MinimalNode('n', {'max_last': 3})
    for i in range(5):
        n.emit('out', i)
    await asyncio.sleep(0.02)
    last = n.get_last()
    assert len(last) == 3
    assert [x['item'] for x in last] == [2, 3, 4]

async def test_set_attribute_truncates_last_items_to_max_last():
    n = _MinimalNode('n', {'max_last': 2})
    for i in range(4):
        n.set_attribute('some_attr', i)
    last = n.get_last()
    assert len(last) == 2
    assert [x['item'] for x in last] == [2, 3]

async def test_set_attribute_refuses_reserved_names():
    n = _MinimalNode('n', {})
    original_config = dict(n.config)
    n.set_attribute('id', 'hacked')
    n.set_attribute('_private', 'hacked')
    assert n.id == 'n'
    assert n.config == original_config

async def test_set_attribute_updates_matching_instance_attribute():
    n = _MinimalNode('n', {})
    n.some_setting = 'old'
    n.set_attribute('some_setting', 'new')
    assert n.some_setting == 'new'
    assert n.config['some_setting'] == 'new'


# ---------- manual_emit() / the 'step'/'emit' handle_control fix ----------

async def test_manual_emit_replays_last_item_per_output_port():
    n = _MinimalNode('n', {})
    out1, out2 = Pipe(), Pipe()
    n.add_output('a', out1)
    n.add_output('b', out2)
    n.emit('a', 'first-a')
    n.emit('b', 'first-b')
    n.emit('a', 'second-a')  # 'a's most recent item is now 'second-a'
    await asyncio.sleep(0.02)
    # Drain what the original emit() calls already put, so only the
    # manual_emit() replay's puts are left to observe below.
    await out1.get(); await out1.get()
    await out2.get()

    replayed = n.manual_emit()
    # Phase 2 of the wire-kind-unification design (see
    # claude/design_unified_wire_kinds_plan.md) retired the generic
    # per-port 'raw_<name>' auto-pairing that add_output()/emit() used to
    # do for every node type (core/node.py's now-deleted
    # _auto_pair_raw_output()) - "raw" is now a per-wire delivery kind on
    # the one real output port a wire is actually drawn to, not a second
    # port automatically created and replayed alongside it. With no
    # raw-kind consumer wired here, manual_emit()'s replay only ever
    # covers the two real ports.
    assert replayed == {'a': 'second-a', 'b': 'first-b'}
    await asyncio.sleep(0.02)
    assert await asyncio.wait_for(out1.get(), timeout=1.0) == 'second-a'
    assert await asyncio.wait_for(out2.get(), timeout=1.0) == 'first-b'

def test_manual_emit_on_a_node_with_no_prior_output_is_a_no_op():
    n = _MinimalNode('n', {})
    n.add_output('out', Pipe())
    assert n.manual_emit() == {}

def test_manual_emit_ignores_attribute_records_but_replays_unwired_ports_too():
    n = _MinimalNode('n', {})
    n.add_output('out', Pipe())
    n.set_attribute('some_attr', 'value')  # recorded as 'attr:some_attr', never a port
    # emit() itself is a safe no-op on an unwired port (records the item,
    # sends it nowhere) - manual_emit() replays the same way, so a port
    # that has emitted before is replayed here whether or not anything is
    # wired to it right now (see manual_emit()'s docstring).
    n.emit('never_wired', 'x')
    replayed = n.manual_emit()
    assert replayed == {'never_wired': 'x'}
    assert 'some_attr' not in replayed and 'attr:some_attr' not in replayed

async def test_handle_control_step_and_emit_both_call_manual_emit():
    # Regression: handle_control() had no 'step' branch at all before this
    # fix, despite POST /nodes/{id}/step (and the MCP node_step tool)
    # having sent {'action': 'step'} control messages since they were
    # added - every "Step" click silently did nothing, on every node
    # type. Both the pre-existing 'step' action and the new 'emit' action
    # (added for the manual-emit title-bar button) now replay the same
    # way via manual_emit().
    for action in ('step', 'emit'):
        n = _MinimalNode('n', {})
        out = Pipe()
        n.add_output('out', out)
        n.emit('out', 'x')
        await asyncio.sleep(0.02)
        await out.get()  # drain the original emit
        await n.handle_control({'action': action})
        assert await asyncio.wait_for(out.get(), timeout=1.0) == 'x'

async def test_handle_control_reset_clears_generic_bookkeeping():
    n = _MinimalNode('n', {})
    n._error_count = 5
    n._last_error = 'boom'
    n._items_in = 10
    n._items_out = 20
    n._bytes_in = 100
    n._bytes_out = 200
    n.emit('out', 'x')
    await asyncio.sleep(0.02)
    await n.handle_control({'action': 'reset'})
    assert n._error_count == 0
    assert n._last_error is None
    assert n._items_in == 0
    assert n._items_out == 0
    assert n._bytes_in == 0
    assert n._bytes_out == 0
    assert n._last_items == []

async def test_reset_does_not_stop_or_reinitialize_the_node():
    # reset() clears accumulated state but must leave the node itself
    # running and its one-time init() untouched - unlike stop()+start().
    n = _MinimalNode('n', {})
    await n.start()
    try:
        assert n._running
        was_initialized = n._initialized
        await n.reset()
        assert n._running
        assert n._initialized == was_initialized
    finally:
        await n.stop()


async def test_stop_then_start_reruns_init_unlike_resume():
    # Bug report: "the generator node couldn't be stopped and started
    # (node top bar doesn't turn green), a click on run is needed to
    # actually turn it on" - reset() didn't help either. Root cause:
    # _initialized used to be set exactly once, ever, and nothing cleared
    # it back to False, so start() after a real stop() behaved exactly
    # like resume() and silently skipped init() - a node whose process()
    # naturally finishes (see the GeneratorInputNode-specific test below)
    # could never be usefully restarted this way. stop() now clears
    # _initialized so a genuine restart reruns init() from scratch, while
    # pause()/resume() (tested separately elsewhere in this file) still
    # correctly leave it alone.
    n = _FlakyNode('n', fail_times=0)
    await n.start()
    try:
        assert n.init_calls == 1
        await n.stop()
        assert n._initialized is False
        await n.start()
        assert n.init_calls == 2
        assert n._initialized is True
    finally:
        await n.stop()


async def test_pause_resume_does_not_rerun_init():
    # The other half of the same fix: pause()/resume() must keep behaving
    # exactly as before - state (and _initialized) survives a pause,
    # unlike a real stop().
    n = _FlakyNode('n', fail_times=0)
    await n.start()
    try:
        assert n.init_calls == 1
        await n.pause()
        await n.resume()
        assert n.init_calls == 1
        assert n._initialized is True
    finally:
        await n.stop()


# ---------- bytes accounting swallow-exceptions paths ----------

class _Unstringable:
    def __str__(self):
        raise RuntimeError('cannot stringify')


async def test_add_input_bytes_in_accounting_survives_unstringable_item():
    n = _MinimalNode('n', {})
    p = Pipe()
    n.add_input('in', p)
    await p.put(_Unstringable())
    item = await n.inputs['in'].get()
    assert isinstance(item, _Unstringable)
    assert n._items_in == 1
    assert n._bytes_in == 0  # str(item) raised, swallowed, byte count skipped

async def test_emit_bytes_out_accounting_survives_unstringable_item():
    n = _MinimalNode('n', {})
    n.emit('out', _Unstringable())
    assert n._items_out == 1
    assert n._bytes_out == 0
