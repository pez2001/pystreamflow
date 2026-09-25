"""
Regression tests for the input-fan-in fix (see
claude/evaluation_and_action_plan.md's write-up for this round): a node's
regular *data* input port used to be backed by exactly one raw pipe,
wired with a plain overwriting ``self.inputs[name] = ...`` - a second
wire landing on the same named input silently stole the connection from
whichever source was wired first, with no error or indication anything
broke. This is the exact same single-slot bug class already fixed for
output ports (Phase 1 of the wire-kind-unification design, "fan-out") and
for the reserved control input (the session that added reset/sessions/
auth, "control-pipe fan-in") - just never fixed on this third side of it,
until this round.

The fix adds ``core/stream.py``'s ``FanInPipe`` and wires it into
``BaseNode.add_input()``/``remove_input()``: a name with exactly one
source is still wrapped directly (zero overhead, unchanged from before
this fix); only once a second source shows up does a name's effective
pipe become a ``FanInPipe`` merging every wired source, transparently, so
every existing node type's own ``process()`` loop (almost all of which
just do ``pipe = self.inputs.get('in'); item = await pipe.get()``) needs
zero changes to correctly receive from more than one source.
"""
import asyncio
import uuid

import pytest

from pystreamflow.core.engine import Engine
from pystreamflow.core.models import Edge, Graph, Node
from pystreamflow.core.node import BaseNode
from pystreamflow.core.stream import FanInPipe, Pipe


def _unique_id(prefix):
    return f'{prefix}-{uuid.uuid4().hex[:8]}'


class _Minimal(BaseNode):
    async def init(self):
        pass
    async def process(self):
        while self._running:
            await asyncio.sleep(1)


# ---------- FanInPipe (core/stream.py) ----------

async def test_faninpipe_delivers_from_every_source():
    fp = FanInPipe()
    a, b = Pipe(), Pipe()
    fp.add_source(a)
    fp.add_source(b)
    await a.put('from-a')
    await b.put('from-b')
    got = {await asyncio.wait_for(fp.get(), timeout=2.0), await asyncio.wait_for(fp.get(), timeout=2.0)}
    assert got == {'from-a', 'from-b'}


async def test_faninpipe_put_is_a_direct_manual_injection():
    # The always-armed internal "manual" source - what send_to_node ends
    # up using once an input has more than one real wire - must be picked
    # up by get() exactly like an item from a real source would be.
    fp = FanInPipe()
    fp.add_source(Pipe())
    await fp.put('manual-item')
    assert await asyncio.wait_for(fp.get(), timeout=2.0) == 'manual-item'


async def test_faninpipe_remove_source_stops_delivery_from_it():
    fp = FanInPipe()
    a, b = Pipe(), Pipe()
    fp.add_source(a)
    fp.add_source(b)
    assert fp.remove_source(a) is True
    await a.put('should-be-ignored')
    await b.put('should-arrive')
    assert await asyncio.wait_for(fp.get(), timeout=2.0) == 'should-arrive'
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fp.get(), timeout=0.3)
    assert fp.remove_source(a) is False  # already gone


async def test_faninpipe_survives_wait_for_timeout_and_retry():
    # Several node types wrap pipe.get() in asyncio.wait_for(..., timeout=
    # ...) and retry on TimeoutError (e.g. LMStudioNode). A FanInPipe's
    # per-source get() tasks must not be torn down and lost when the
    # *outer* wait_for cancels the FanInPipe.get() call itself - the next
    # get() call has to pick up an item that arrives after the timeout,
    # not miss it.
    fp = FanInPipe()
    a = Pipe()
    fp.add_source(a)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(fp.get(), timeout=0.05)
    await a.put('arrived-late')
    assert await asyncio.wait_for(fp.get(), timeout=2.0) == 'arrived-late'


def test_faninpipe_stats_reports_fan_in_count():
    fp = FanInPipe()
    fp.add_source(Pipe())
    fp.add_source(Pipe())
    st = fp.stats()
    assert st['fan_in'] == 2
    assert 'size' in st and 'dropped' in st


# ---------- BaseNode.add_input()/remove_input() ----------

async def test_add_input_appends_instead_of_overwriting():
    n = _Minimal('n', {})
    first, second = Pipe(), Pipe()
    n.add_input('in', first)
    n.add_input('in', second)
    assert n._input_sources['in'] == [first, second]
    await first.put('from-first')
    await second.put('from-second')
    got = {await asyncio.wait_for(n.inputs['in'].get(), timeout=2.0),
           await asyncio.wait_for(n.inputs['in'].get(), timeout=2.0)}
    assert got == {'from-first', 'from-second'}


def test_add_input_single_source_skips_faninpipe():
    # The common case (the overwhelming majority of wiring in this
    # codebase) must stay on the original, un-merged fast path - no
    # FanInPipe object should be created at all until a second source
    # actually shows up.
    n = _Minimal('n', {})
    n.add_input('in', Pipe())
    assert 'in' not in n._input_fanin


async def test_remove_input_removes_only_the_named_source():
    n = _Minimal('n', {})
    first, second = Pipe(), Pipe()
    n.add_input('in', first)
    n.add_input('in', second)
    assert n.remove_input('in', first) is True
    assert n._input_sources['in'] == [second]
    # Collapsed back down to one source - the FanInPipe wrapper should be
    # torn down, putting this port back on the un-merged fast path.
    assert 'in' not in n._input_fanin
    await second.put('still-here')
    assert await asyncio.wait_for(n.inputs['in'].get(), timeout=2.0) == 'still-here'
    # Removing the last remaining source drops the port entirely, same as
    # the old "nothing wired here" state.
    assert n.remove_input('in', second) is True
    assert 'in' not in n.inputs
    assert 'in' not in n._input_sources
    # Removing again (already gone), an unknown pipe, or an unwired port
    # is a no-op, not an error - and 'control' is explicitly out of scope
    # (remove_control_input() is the async counterpart for that).
    assert n.remove_input('in', first) is False
    assert n.remove_input('never-wired', Pipe()) is False
    assert n.remove_input('control', Pipe()) is False


async def test_three_sources_then_remove_one_leaves_faninpipe_with_two():
    n = _Minimal('n', {})
    a, b, c = Pipe(), Pipe(), Pipe()
    n.add_input('in', a)
    n.add_input('in', b)
    n.add_input('in', c)
    assert n.remove_input('in', a) is True
    # Two sources remain - still above the single-source threshold, so the
    # FanInPipe wrapper must stay in place (not torn down prematurely).
    assert 'in' in n._input_fanin
    await b.put('from-b')
    await c.put('from-c')
    got = {await asyncio.wait_for(n.inputs['in'].get(), timeout=2.0),
           await asyncio.wait_for(n.inputs['in'].get(), timeout=2.0)}
    assert got == {'from-b', 'from-c'}


def test_add_input_if_unwired_placeholder_is_evicted_not_fanned_in():
    # Mirrors add_output_if_unwired()'s own eviction fix (Phase 1): a
    # "self-contained" node's own internal placeholder pipe (created in
    # init() because nothing real had been wired yet) must be replaced,
    # not merged in alongside, the first *real* wire that arrives later -
    # otherwise every such node type would carry a permanently-idle
    # phantom fan-in source (and its listener task) forever, and report an
    # inflated fan_in count that doesn't reflect any real source.
    n = _Minimal('n', {})
    placeholder = Pipe()
    result = n.add_input_if_unwired('in', placeholder)
    assert result is n.inputs['in']
    assert n._input_sources['in'] == [placeholder]

    real_pipe = Pipe()
    n.add_input('in', real_pipe)
    # The placeholder must be gone, not fanned in alongside the real wire.
    assert n._input_sources['in'] == [real_pipe]
    assert 'in' not in n._input_fanin

    # A *second* real wire arriving after that must genuinely fan in
    # (this is not the placeholder-eviction case any more).
    second_real = Pipe()
    n.add_input('in', second_real)
    assert n._input_sources['in'] == [real_pipe, second_real]
    assert 'in' in n._input_fanin


# ---------- End-to-end via a real Engine-run graph (YAML-equivalent wiring) ----------

async def test_engine_wires_two_edges_into_the_same_input_and_both_deliver():
    # Engine._wire_edges() calls add_input() once per edge with no
    # special-casing needed for fan-in - the fix lives entirely in
    # BaseNode, so a workflow graph with two edges into the same named
    # input port (exactly what a hand-written or generated workflow YAML
    # could already declare, previously wiring the second edge only to
    # silently steal the connection from the first) must now deliver both.
    # repeat=True/interval=0.05 rather than a single one-shot emission:
    # nothing is wired to sink's own 'out' until after the engine is
    # already running (the probe below has to be added post-hoc), so a
    # one-shot emit from either source could easily fire - and be lost,
    # since emit() on a consumer-less port is a no-op - before the probe
    # ever gets attached. Repeating emission sidesteps that startup race
    # entirely: the assertions below just wait for the *next* round from
    # each source once the probe is live, proving genuine ongoing
    # multi-source delivery through the merged input rather than a single
    # racy snapshot.
    g = Graph()
    g.add_node(Node(id='src1', type='ConstantValueNode', config={'value': 'one', 'repeat': True, 'interval': 0.05}))
    g.add_node(Node(id='src2', type='ConstantValueNode', config={'value': 'two', 'repeat': True, 'interval': 0.05}))
    g.add_node(Node(id='sink', type='LogOutputNode', config={}))
    g.add_edge(Edge(source='src1', target='sink', source_port='out', target_port='in', type='data'))
    g.add_edge(Edge(source='src2', target='sink', source_port='out', target_port='in', type='data'))
    engine = Engine(g)
    engine.validate()
    task = asyncio.create_task(engine.run())
    try:
        await asyncio.sleep(0.1)
        sink = engine.node_instances['sink']
        assert len(sink._input_sources.get('in', [])) == 2
        # LogOutputNode re-emits {'level', 'log': <received item>} on 'out'
        # for whatever it received on 'in' - probe that the same way
        # test_api_server_coverage.py's fan-out tests already do.
        probe = Pipe()
        sink.add_output('out', probe)
        seen = set()
        for _ in range(20):
            got = await asyncio.wait_for(probe.get(), timeout=2.0)
            seen.add(got['log'])
            if {'one', 'two'} <= seen:
                break
        assert {'one', 'two'} <= seen
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        for nid, node in list(engine.node_instances.items()):
            await node.stop()
