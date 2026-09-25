import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from .stream import FanInPipe, Pipe

logger = logging.getLogger("pystreamflow.node")


class BaseNode(ABC):
    """Base class for all PyStreamFlow nodes.
    Provides lifecycle, stats, control handling and pipe management.

    Lifecycle contract for subclasses
    ----------------------------------
    - Override ``async def init(self)`` for one-time setup (opening a
      connection, compiling a script, loading a file, ...). It runs at
      most once per node instance - the first time the node starts - and
      is never re-run by pause()/resume() or by later start() calls, so
      state set up there survives a pause/resume cycle.
    - Override ``async def process(self)`` (required) with the node's main
      loop, written as ``while self._running: ...``. It's expected to keep
      running until ``self._running`` goes False or its task is cancelled;
      returning early is also fine (e.g. a one-shot node).

    ``start()`` awaits one-time setup (``init()``) synchronously - so
    anything init() sets up (ports added via add_input/add_output, a
    compiled script, an opened connection, ...) is guaranteed to exist by
    the time ``start()`` returns - and then schedules the long-running
    ``process()`` loop as a tracked background task (``self._task``)
    rather than awaiting it inline. That inline await used to be exactly
    what ``start()`` did with *everything*, init() included, so anything
    that called ``await node.start()`` without independently wrapping that
    call in ``asyncio.create_task(...)`` would deadlock forever the moment
    ``process()`` looped (which is nearly always, by the contract above).
    Every real caller in this codebase already wrapped it in
    ``create_task`` - only a chunk of the test suite called it directly
    and hung - but it was a footgun waiting for the next caller, and it's
    fixed now: calling ``start()`` directly and ``await``-ing it is safe
    and returns promptly. ``stop()`` / ``pause()`` / ``resume()`` are the
    same - schedule-and-return, never blocking on process() itself.
    """

    def __init__(self, node_id: str, config: dict[str, Any] | None = None):
        self.id = node_id
        self.config = config or {}
        # Set by Engine._instantiate_nodes() to the owning Session's id
        # for any node created through a Session/Engine run; stays None
        # for a node built directly (a unit test, the ad-hoc single-graph
        # node editor's own /nodes endpoints, an MCP create_node call with
        # no session). core/web_server.py's register_node() reads this to
        # keep concurrent sessions' nodes from colliding in the global
        # node-by-id registry when two sessions happen to reuse the same
        # node id - see that function's docstring for the bug this fixes.
        self.session_id: str | None = None
        self.inputs: dict[str, Pipe] = {}
        # Fan-in fix (see add_input()/remove_input()/FanInPipe in
        # core/stream.py): a regular *data* input port used to be backed
        # by exactly one raw pipe, set with a plain overwriting
        # `self.inputs[name] = ...` - the same single-slot bug already
        # fixed on the output side (self.outputs below) and the control
        # side (self._control_pipes below), just never fixed here. These
        # two dicts are the bookkeeping that makes it possible: every raw
        # source pipe ever wired to a given input name (excluding
        # 'control', which has its own separate list below), and - once a
        # name has two or more of them - the FanInPipe merging them into
        # the single effective object self.inputs[name] actually wraps.
        # A name with exactly one source skips the FanInPipe entirely and
        # wraps that one raw pipe directly, exactly as before this fix -
        # see add_input()'s own docstring for why that fast path matters.
        self._input_sources: dict[str, list[Pipe]] = {}
        self._input_fanin: dict[str, FanInPipe] = {}
        # Mirrors self._fallback_output_pipes below, for the input side:
        # tracks, per input name, the internal placeholder pipe (if any)
        # add_input_if_unwired() created because nothing real was wired
        # there yet. Needed for the same reason - see add_input()'s own
        # comment on evicting rather than fanning-in this placeholder the
        # moment a real source arrives.
        self._fallback_input_pipes: dict[str, Pipe] = {}
        # Phase 1 of the wire-kind-unification design (see
        # claude/design_unified_wire_kinds_plan.md): this used to be
        # dict[str, Pipe] - one output port could only ever feed exactly
        # one downstream consumer, since add_output() below did a plain
        # overwriting `self.outputs[name] = pipe`. A second wire drawn
        # from the same output port silently stole the first consumer's
        # pipe with no error or indication anything went wrong. Now each
        # port name holds a list of every live consumer, so a single
        # output can legitimately fan out to several downstream nodes.
        #
        # Phase 2 (same design doc): each consumer entry is now a
        # ``(Pipe, kind)`` pair, ``kind`` one of ``"data"``/``"raw"``,
        # instead of a bare ``Pipe`` - a single output anchor can now feed
        # some consumers the item as-is (``"data"``) and others its
        # unwrapped bare value (``"raw"``), chosen per wire rather than by
        # which separate, permanently-duplicated port name a wire happens
        # to land on (the retired ``_auto_pair_raw_output()`` scheme). See
        # add_output()/remove_output()/emit() below for the rest of this.
        self.outputs: dict[str, list[tuple[Pipe, str]]] = {}
        # Tracks, per output port name, the internal placeholder pipe (if
        # any) add_output_if_unwired() created because nothing real was
        # wired there yet - see that method's docstring and add_output()'s
        # own comment on why this needs to be evicted, not appended
        # alongside, the moment a real consumer shows up.
        self._fallback_output_pipes: dict[str, Pipe] = {}
        self._running = False
        self._last_items: list[Any] = []
        self._max_last = int(self.config.get('max_last', 100))
        self._health = 'unknown'
        self._error_count = 0
        self._last_error = None
        self._auto_start = bool(self.config.get('auto_start', True))
        self._started = False        # True once add_input has triggered auto-start
        self._initialized = False    # True once init() has run, ever
        self._paused = False
        # A node's control input used to be a single overwritable slot
        # (self._control_pipe = one Pipe) - a second control edge wired
        # onto the same target silently stole control away from whatever
        # source was wired first, with no error or indication anything
        # broke. That's the exact same bug class Phase 1 of the
        # wire-kind-unification design already fixed on the *data*-output
        # side (see self.outputs' docstring above) - flagged as its own
        # open question in that design doc and picked up here. Every
        # control source now keeps its own independent listener task (see
        # _ensure_control_listener()/add_input()/remove_control_input()
        # below), so a second (or third, ...) control wire genuinely adds
        # a second live source instead of replacing the first.
        self._control_pipes: list[Pipe] = []
        self._control_listeners: dict[int, asyncio.Task] = {}  # id(pipe) -> its listener task
        self._task: asyncio.Task | None = None          # the process() loop
        self._emit_tasks: set[asyncio.Task] = set()      # in-flight emit() puts
        # Populated by Engine._wire_edges() with the target node id(s) of
        # any real `type: control` edge in the graph that originates from
        # this node - see core/trigger_targets.py's TriggerActionMixin,
        # which resolves a trigger's target from here first and only
        # falls back to a hand-typed `target_node_id` config field if this
        # is empty (Phase 2 fix for the "control wires are cosmetic"
        # finding in the project's evaluation doc).
        self._graph_control_targets: list[str] = []
        # stats
        self._items_in = 0
        self._items_out = 0
        self._bytes_in = 0
        self._bytes_out = 0
        self._start_ts = None

    async def init(self):
        pass

    @abstractmethod
    async def process(self):
        pass

    async def _ensure_started(self):
        if self._started:
            return
        self._started = True
        await self.start()

    async def start(self):
        """Start (or resume) the node.

        Runs one-time setup (``init()``) synchronously - so callers can
        rely on anything init() sets up (like ports added via
        ``add_input``/``add_output``) existing the moment this returns -
        then schedules the long-running ``process()`` loop as a tracked
        background task and returns immediately, rather than blocking
        until that loop finishes (which, per the process() contract above,
        is normally "never" until stopped). Safe to call more than once; a
        no-op if the node is already running. ``init()`` only re-runs when
        ``self._initialized`` is ``False`` - true for a brand new instance,
        and true again after a real ``stop()`` (which clears it - see that
        method's own docstring) - so resuming a *paused* node (``resume()``,
        which never touches ``_initialized``) doesn't redo one-time setup
        and keeps whatever state ``process()`` left behind, while restarting
        a *stopped* one genuinely starts fresh.

        Bug fix (real-world report: a workflow with an unrelated
        misconfigured node - a ``UrlInputNode`` with no ``url`` set -
        crashed its *entire* session, not just that one node):
        ``Engine.run()``'s node-startup loop calls ``await
        self.node_instances[nid].start()`` for every node in the graph,
        one after another, with no per-node isolation - so a bare
        ``await self.init()`` here that raises used to propagate straight
        out of that loop and abort ``Engine.run()`` entirely, before it
        ever reached ``_supervise()``. That took down every *other* node
        in the graph too: anything already started (earlier in topological
        order) kept running as an orphaned, no-longer-supervised task
        forever (nothing downstream of the crash ever called
        ``stop_all()``), and anything not yet started (later in
        topological order) never got a chance to start at all - one typo
        in one node's config could silently kill a whole otherwise-working
        workflow. ``_run_loop()`` below already treats a *retry* path's
        re-``init()`` failure this same forgiving way (log it, mark this
        one node ``'error'``, leave every other node alone) - this makes
        the very first ``init()`` call behave identically instead of being
        the one uncaught case.
        """
        if self._running:
            return self._task
        self._running = True
        self._paused = False
        self._health = 'starting'
        for _control_pipe in self._control_pipes:
            self._ensure_control_listener(_control_pipe)
        if not self._initialized:
            try:
                await self.init()
                self._initialized = True
            except asyncio.CancelledError:
                self._running = False
                self._health = 'stopped'
                raise
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                self._health = 'error'
                self._running = False
                logger.exception(
                    "node %s (%s): init() failed - this node will not start "
                    "(other nodes in the same workflow are unaffected)",
                    self.id, type(self).__name__,
                )
                return None
        self._start_ts = time.monotonic()
        self._health = 'healthy'
        self._task = asyncio.create_task(self._run_loop())
        return self._task

    async def _run_loop(self):
        """Runs process(), retrying (with a fresh init()) on error."""
        retries = int(self.config.get('retries', 0))
        delay = float(self.config.get('retry_delay', 1.0))
        attempt = 0
        while True:
            try:
                await self.process()
                self._health = 'stopped' if not self._running else 'idle'
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._error_count += 1
                self._last_error = str(e)
                logger.exception(
                    "node %s (%s) raised in process()", self.id, type(self).__name__
                )
                if attempt >= retries:
                    self._health = 'error'
                    return
                attempt += 1
                self._health = f'retrying_{attempt}'
                await asyncio.sleep(delay * attempt)
                try:
                    await self.init()
                except asyncio.CancelledError:
                    raise
                except Exception as e2:
                    self._error_count += 1
                    self._last_error = str(e2)
                    logger.exception(
                        "node %s (%s): re-init before retry failed",
                        self.id, type(self).__name__,
                    )

    async def stop(self):
        """Stop the node: cancel its loop (and every control listener), mark
        it not running, and clear ``_initialized`` so a later ``start()``
        redoes one-time setup from scratch - unlike ``pause()``, which
        leaves ``_initialized`` alone specifically so ``resume()`` picks up
        exactly where it left off (see that method's own docstring). This
        is the real distinction between "stop" and "pause" this class has
        always documented (``test_reset_does_not_stop_or_reinitialize_the_node``
        contrasts ``reset()`` with "unlike stop()+start()", implying
        stop()+start() re-initializes) but never actually implemented -
        ``_initialized`` used to be set exactly once, ever, and nothing
        anywhere ever cleared it back to ``False``, so a stopped node
        restarted via ``start()`` skipped ``init()`` and silently resumed
        with whatever state ``process()`` left behind, identically to
        ``resume()``.

        Bug report: "the generator node couldn't be stopped and started
        (node top bar doesn't turn green), a click on run is needed to
        actually turn it on" - reproduced live: ``GeneratorInputNode``
        emits `count` items then returns from ``process()`` on its own
        (``self.i`` reaches ``self.count``); with ``init()`` never re-run,
        ``self.i`` stayed at ``count`` forever, so every subsequent
        ``start()`` immediately re-entered and instantly exited
        ``process()``'s ``while ... self.i < self.count`` loop - the title
        bar never had a chance to read as running, and (a related report
        from the same testing session) nothing new ever reached whatever
        was wired downstream of it either. Only the full graph "Run"
        button worked, because that always builds brand new node
        instances (`Engine._instantiate_nodes()`) rather than restarting
        an exhausted one. Confirmed a plain ``reset()`` didn't help either
        - by design, `reset()` deliberately leaves one-time setup and any
        node-specific state like `self.i` untouched (see its own
        docstring) - only a real `stop()` was ever supposed to clear that.
        """
        self._running = False
        self._paused = False
        self._health = 'stopped'
        self._initialized = False
        await self._cancel_task('_task')
        await self._cancel_all_control_listeners()

    async def pause(self):
        """Suspend the node's processing loop without discarding its state.

        Unlike stop(), this leaves init() as already-done, so resume()
        re-enters process() rather than re-initializing the node - any
        state the node keeps on ``self`` (counters, buffers, connections
        opened in init(), ...) survives the pause. The control listener
        keeps running so a later 'resume'/'stop' control message can still
        reach the node.
        """
        if not self._running or self._paused:
            return
        self._paused = True
        self._running = False
        self._health = 'paused'
        await self._cancel_task('_task')

    async def resume(self):
        """Resume a paused node."""
        if not self._paused:
            return
        self._paused = False
        await self.start()

    async def _cancel_task(self, attr: str):
        task = getattr(self, attr, None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("node %s: error while cancelling %s", self.id, attr)
        setattr(self, attr, None)

    def _ensure_control_listener(self, pipe: Pipe):
        """Start (or restart, if it previously exited - e.g. after the
        handler raised, see _control_listener()) the background task that
        reads control messages off exactly one control-edge pipe and
        dispatches them to handle_control(). Keyed by id(pipe) so several
        concurrently-wired control sources each get, and keep, their own
        independent listener - see the fan-in fix noted on
        self._control_pipes in __init__.
        """
        key = id(pipe)
        task = self._control_listeners.get(key)
        if task is None or task.done():
            self._control_listeners[key] = asyncio.create_task(self._control_listener(pipe))

    async def _cancel_all_control_listeners(self):
        """Cancel every per-pipe control listener task started by
        _ensure_control_listener(), mirroring _cancel_task()'s own
        cancel-and-await-completion contract but generalized to however
        many control sources are currently wired.
        """
        tasks = list(self._control_listeners.values())
        self._control_listeners.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("node %s: error while cancelling a control listener", self.id)

    def health(self):
        """Return health and stats snapshot for the node."""
        now = time.monotonic()
        uptime = max(0.0, now - self._start_ts) if self._start_ts else 0.0
        items_in_rate = self._items_in / uptime if uptime > 0 else 0.0
        items_out_rate = self._items_out / uptime if uptime > 0 else 0.0
        return {
            'id': self.id,
            'health': self._health,
            'running': self._running,
            'paused': self._paused,
            'error_count': self._error_count,
            'last_error': self._last_error,
            'uptime_s': round(uptime, 3),
            'stats': {
                'items_in': self._items_in,
                'items_out': self._items_out,
                'bytes_in': self._bytes_in,
                'bytes_out': self._bytes_out,
                'items_in_rate': round(items_in_rate, 3),
                'items_out_rate': round(items_out_rate, 3),
            },
            'inputs': {k: v.stats() for k,v in self.inputs.items()},
            # Phase 1 fan-out change: each output port can now hold several
            # live consumer pipes, so its stats are reported as a list (one
            # entry per consumer) instead of a single dict - see
            # self.outputs' own docstring in __init__ above. Phase 2 adds
            # each consumer's delivery kind ("data"/"raw") alongside its
            # stats, since a port's consumers are no longer all the same.
            'outputs': {
                k: [{**pipe.stats(), 'kind': kind} for pipe, kind in pipes]
                for k, pipes in self.outputs.items()
            },
        }

    def add_input(self, name: str, pipe: Pipe):
        """Wire ``pipe`` as an additional live source of data input port
        ``name``, alongside any other pipe(s) already wired there.

        Fan-in fix (see ``core/stream.py``'s ``FanInPipe`` and
        ``self._input_sources``'s docstring in ``__init__``): this used to
        be a plain overwriting ``self.inputs[name] = <wrapped pipe>`` - a
        second wire landing on the same input port name silently replaced
        the first source with no error or indication, exactly the same
        bug class already fixed for outputs (``add_output()``) and
        control wires (the ``name == 'control'`` branch just below). A
        name with exactly one source is still wrapped directly, with zero
        overhead versus the pre-fix behavior - only once a second source
        actually shows up does this reach for a ``FanInPipe`` to merge
        them, so the overwhelmingly common single-source case (still true
        of the vast majority of this codebase's ~90 node types) never
        pays for it. See ``remove_input()`` for the matching disconnect
        side.

        One wrinkle this fix introduces on its own, mirroring
        ``add_output()``'s own ``self._fallback_output_pipes`` comment:
        several "self-contained" node types call
        ``add_input_if_unwired()`` in their own ``init()`` to give
        themselves a usable internal pipe when nothing real has been
        wired yet (see that method's docstring). In the ad-hoc
        node-editor/API/MCP flow specifically, a node can be created and
        auto-started - running init() and creating that placeholder -
        *before* a real wire arrives via a later ``POST /nodes/connect``
        call. Without special-casing it, that real wire would count as a
        second source and get merged in fan-in-style alongside the
        placeholder - which nothing ever puts into - inflating the
        reported fan-in count and leaving one permanently-idle phantom
        source and its listener task behind forever. Evicting the
        placeholder here - the moment a *real* pipe is added for a name
        that currently holds only it - restores "real wiring wins"
        instead.
        """
        if name == 'control':
            # Fan-in fix (see self._control_pipes' docstring in __init__):
            # append instead of the old plain overwrite, so a second
            # control edge onto this node is a genuine second live source
            # rather than a silent takeover of the first.
            self._control_pipes.append(pipe)
            if self._running or self._paused:
                # A control wire drawn onto an already-started node (the
                # live node-editor case) needs its listener spun up right
                # now - start() only sees the control pipes present at the
                # moment it ran.
                self._ensure_control_listener(pipe)
            return
        sources = self._input_sources.setdefault(name, [])
        fallback = self._fallback_input_pipes.pop(name, None)
        if fallback is not None and sources == [fallback]:
            sources.clear()
            self._input_fanin.pop(name, None)
        sources.append(pipe)
        if len(sources) == 1:
            effective: Pipe = pipe
        else:
            fan_in = self._input_fanin.get(name)
            if fan_in is None:
                fan_in = FanInPipe()
                fan_in.add_source(sources[0])
                self._input_fanin[name] = fan_in
            fan_in.add_source(pipe)
            effective = fan_in
        self._store_wrapped_input(name, effective)

    def _store_wrapped_input(self, name: str, effective_pipe: Pipe) -> None:
        """Wrap ``effective_pipe`` (a raw single-source ``Pipe``, or a
        ``FanInPipe`` merging several) for auto-start/stat-counting and
        install it as ``self.inputs[name]`` - the wrapping logic
        ``add_input()`` always used to do inline before the fan-in fix
        above split it out so ``remove_input()`` could reuse it too (when
        a name collapses back down from a FanInPipe to a single remaining
        raw source).
        """
        node = self
        original_get = effective_pipe.get
        async def counted_get():
            item = await original_get()
            node._items_in += 1
            try:
                node._bytes_in += len(str(item).encode('utf-8'))
            except Exception:
                pass
            return item
        if self._auto_start:
            async def auto_get():
                if not node._started:
                    await node._ensure_started()
                return await counted_get()
            class _AutoPipe:
                def __init__(self, p):
                    self._p = p
                async def get(self):
                    return await auto_get()
                async def put(self, item):
                    return await self._p.put(item)
                def stats(self):
                    return self._p.stats()
            self.inputs[name] = _AutoPipe(effective_pipe)
        else:
            class _CountingPipe:
                def __init__(self, p):
                    self._p = p
                async def get(self):
                    return await counted_get()
                async def put(self, item):
                    return await self._p.put(item)
                def stats(self):
                    return self._p.stats()
            self.inputs[name] = _CountingPipe(effective_pipe)

    def remove_input(self, name: str, pipe: Pipe) -> bool:
        """Remove exactly one source pipe previously added via
        ``add_input()`` from data input port ``name``, leaving any other
        source still wired to the same port untouched. Returns ``True``
        if a source was actually removed, ``False`` if ``name`` isn't
        wired at all or ``pipe`` isn't (or is no longer) one of its
        sources.

        The fan-in counterpart to ``remove_output()``/
        ``remove_control_input()``. Before the fan-in fix on ``add_input``
        above, a data input's only backing store was a single wrapped
        pipe, so disconnecting meant unconditionally deleting ``name``
        from ``self.inputs`` outright (see e.g. ``api/server.py``'s
        ``disconnect_nodes()``, which now calls this instead) - correct
        only by accident, since a second source could never have coexisted
        with the first anyway under the old overwrite semantics, so there
        was never anything else to preserve.

        When this brings a name down from two-or-more sources to exactly
        one, the ``FanInPipe`` wrapper is torn down and the port is
        rewrapped directly onto that one remaining raw pipe - putting the
        common single-source case back on its original, un-merged fast
        path once fan-in is no longer needed, mirroring ``add_input()``'s
        own single-vs-merged branch.
        """
        if name == 'control':
            return False
        sources = self._input_sources.get(name)
        if not sources:
            return False
        try:
            sources.remove(pipe)
        except ValueError:
            return False
        fan_in = self._input_fanin.get(name)
        if fan_in is not None:
            fan_in.remove_source(pipe)
        if not sources:
            del self._input_sources[name]
            self._input_fanin.pop(name, None)
            self.inputs.pop(name, None)
            return True
        if fan_in is not None and len(sources) == 1:
            del self._input_fanin[name]
            self._store_wrapped_input(name, sources[0])
        return True

    def add_output(self, name: str, pipe: Pipe, kind: str = "data"):
        """Wire ``pipe`` as an additional live consumer of output port
        ``name``, alongside any other pipe(s) already wired there.
        ``kind`` (``"data"`` or ``"raw"``) is this consumer's own delivery
        kind - see ``emit()`` below for what each one actually does.

        Phase 1 fan-out fix: this used to be a plain overwriting
        ``self.outputs[name] = pipe`` - the second wire drawn from the same
        output port name (which is all ``editor.js`` ever sends as
        ``source_port``, with no per-wire disambiguation) silently replaced
        the first consumer's pipe instead of adding a second one, breaking
        the first consumer with no error or indication. Now appends, so a
        single output can legitimately feed multiple downstream nodes. See
        ``remove_output()`` for the matching disconnect-side fix.

        One wrinkle the append-not-overwrite change introduces on its own:
        several "self-contained" node types (see
        ``add_output_if_unwired()``'s docstring for the full list) call
        that method in ``init()`` to give themselves a usable internal
        pipe before any real wiring exists - for the live node-editor/MCP
        flow specifically, that init() runs *before* a wire is drawn, so
        it always creates that internal placeholder first. Under the old
        overwrite semantics, a real ``add_output()`` call arriving later
        cleanly replaced it; under append semantics it would otherwise
        stick around forever as a permanent, silent, never-drained extra
        consumer of every future ``emit()`` on this port. Evicting it here
        - the moment a *real* pipe is added for the same name - restores
        the original "real wiring wins" behavior without giving up the
        new ability to fan out to more than one real consumer.

        Phase 2 of the wire-kind-unification design (see
        ``claude/design_unified_wire_kinds_plan.md``): retired the
        separate, permanently-duplicated "raw" output *port* in favor of
        this ``kind`` parameter - a single output anchor can now feed some
        consumers ``"data"`` and others ``"raw"``, chosen per wire.
        """
        fallback = self._fallback_output_pipes.pop(name, None)
        if fallback is not None and self.outputs.get(name) == [(fallback, "data")]:
            self.outputs[name] = []
        self.outputs.setdefault(name, []).append((pipe, kind))

    def remove_output(self, name: str, pipe: Pipe) -> bool:
        """Remove exactly one consumer pipe previously added via
        ``add_output()``/``add_output_if_unwired()`` from output port
        ``name``, leaving any other consumer of the same port untouched.
        Returns ``True`` if a pipe was actually removed, ``False`` if
        ``name`` isn't wired at all or ``pipe`` isn't (or is no longer) one
        of its consumers.

        Phase 1 counterpart to the ``add_output()`` fan-out fix above:
        disconnecting used to mean ``del self.outputs[name]`` - fine when a
        port could only ever have one consumer, wrong now that it can have
        several, since that would silently kill every other live consumer
        of the same port along with the one edge actually being removed.
        Matches by the pipe's identity only - its delivery kind (Phase 2)
        doesn't matter for removal, only for what emit() does with it.
        """
        pipes = self.outputs.get(name)
        if not pipes:
            return False
        for i, (p, _kind) in enumerate(pipes):
            if p is pipe:
                del pipes[i]
                if not pipes:
                    del self.outputs[name]
                return True
        return False

    async def remove_control_input(self, pipe: Pipe) -> bool:
        """Remove exactly one control-edge pipe previously added via
        ``add_input('control', pipe)``, leaving any other wired control
        source untouched - the disconnect-side counterpart to the fan-in
        fix on ``self._control_pipes`` (see ``__init__``'s comment and
        ``add_input()`` above). Before this, disconnecting a control edge
        meant ``tgt._control_pipe = None``, which wiped *every* control
        source a node had, not just the one edge actually being removed.

        Cancels and awaits that pipe's own listener task if one is
        running, mirroring ``remove_output()``'s "match by identity, only
        touch this one consumer" contract. Returns ``True`` if a pipe was
        actually removed, ``False`` if it wasn't (or is no longer) wired.
        """
        try:
            self._control_pipes.remove(pipe)
        except ValueError:
            return False
        task = self._control_listeners.pop(id(pipe), None)
        if task is not None and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("node %s: error while cancelling a control listener", self.id)
        return True

    def add_output_if_unwired(self, name: str, pipe: Pipe) -> Pipe:
        """Register ``pipe`` as this node's output on ``name`` only if
        nothing has wired that port yet; otherwise leave the existing
        wiring alone. Returns whichever pipe is actually in effect for
        ``name`` afterward (the one just passed, or whatever was already
        there).

        Phase 6 hardening fix: several "self-contained" node types (ones
        that create their own internal output pipe in init() so they still
        work when driven directly, e.g. in a test, without an Engine)
        used to call the plain add_output() above unconditionally. That's
        a real bug, not just a style nit: Engine._wire_edges() always
        wires a node's *real* downstream output pipe via add_output()
        first, and only *then* starts the node - and starting a node is
        what runs init() (see start(), which calls init() before
        scheduling process()). So an unconditional add_output() call
        inside init() silently overwrote the Engine's real wiring with the
        node's own internal pipe every time, on every node type that did
        this - the node would run fine and emit successfully in isolation,
        but its output could never reach anything actually wired
        downstream of it in a real graph. First found and fixed on
        SubgraphNode in Phase 5; the Phase 6 hardening pass found the same
        pattern independently repeated in SocketInputNode, MQTTInputNode,
        LMStudioNode, WebInputNode, UserInputNode, ScriptedOutputNode and
        ScriptInputNode, all fixed the same way - by switching their
        init() to call this method instead of add_output() directly.

        Phase 1 fan-out change: ``self.outputs[name]`` is now a list of
        every live consumer pipe rather than a single ``Pipe`` (see
        ``add_output()``'s docstring), but this method's own contract is
        unchanged - it still returns a single, directly usable ``Pipe``
        (the one just passed when nothing was wired yet, or the first real
        consumer's pipe if something already was), which is what every
        caller of this method actually uses: several "self-contained" node
        types stash the return value as ``self.out_pipe``/``self.pipe``
        purely so a unit test with no Engine can ``await
        node.out_pipe.get()`` directly. When a port already has more than
        one live consumer by the time this is called (only possible for a
        node type that also gets external fan-out wiring - not a pattern
        any current caller of this method exercises), returning just the
        first one is an arbitrary but harmless choice, since none of these
        callers ever ``put()``s through the returned pipe in real
        (non-test) operation - real delivery always goes through
        ``emit()``, which iterates every pipe under the name.

        Remembers ``pipe`` in ``self._fallback_output_pipes`` when it's the
        one actually stored (nothing was wired yet) - see ``add_output()``'s
        docstring for why that record exists: so a real wire arriving later
        for this same port evicts this placeholder instead of appending
        alongside it forever. Always registered as a ``"data"``-kind
        consumer (Phase 2) - a placeholder pipe is a stand-in for "nothing
        real wired yet," never itself a raw-delivery consumer.
        """
        if not self.outputs.get(name):
            self.outputs[name] = [(pipe, "data")]
            self._fallback_output_pipes[name] = pipe
        return self.outputs[name][0][0]

    def add_input_if_unwired(self, name: str, pipe: Pipe) -> Pipe:
        """Register ``pipe`` as this node's input on ``name`` only if
        nothing has wired that port yet; otherwise leave the existing
        wiring alone. Returns whichever pipe is actually in effect for
        ``name`` afterward (the one just passed, or whatever was already
        there) - already wrapped for auto-start/stat-counting exactly as
        add_input() would wrap it.

        Mirrors add_output_if_unwired() above, but for the input side.
        WebOutputNode and WebOutputJSONNode each create their own internal
        ``Pipe()`` in init() and call add_input() on it unconditionally so
        they still work when driven directly (e.g. in a test) without an
        Engine - but Engine._wire_edges() always wires a node's *real*
        upstream output into this node's input first, and only *then*
        starts the node (which is what runs init()). An unconditional
        add_input() in init() therefore silently overwrote the Engine's
        real wiring with the node's own disconnected internal pipe every
        time - the node would run fine and forward whatever was put into
        its own pipe directly, but never actually receive anything from
        whatever was really wired upstream of it in a graph run through
        the Engine. Same bug class add_output_if_unwired() already fixed
        for several node types' outputs, just never applied to an input.

        Records ``pipe`` in ``self._fallback_input_pipes`` when it's the
        one actually stored (nothing was wired yet) - see ``add_input()``'s
        own docstring for why: so a real wire arriving later for this same
        name evicts this placeholder instead of fan-in-merging it in
        alongside a source nothing will ever actually put through.
        """
        if name not in self.inputs:
            self.add_input(name, pipe)
            self._fallback_input_pipes[name] = pipe
        return self.inputs[name]

    # Attribute names a node's own core lifecycle machinery relies on -
    # set_attribute() refuses to touch these no matter what an incoming
    # 'attribute' wire is named, so a wire can never clobber the node's
    # id, its raw config dict, or any internal (`_`-prefixed) state.
    _RESERVED_ATTRIBUTE_NAMES = frozenset({"id", "config", "inputs", "outputs"})

    def set_attribute(self, name: str, value: Any) -> None:
        """Live-update one config attribute from an incoming ``type:
        attribute`` graph edge (see ``Engine._pump_attribute``) - e.g.
        wiring a small settings node's output into a ``FileInputNode``'s
        ``path``, instead of that value only ever coming from a static
        config field typed in once when the workflow was built.

        This updates ``self.config[name]`` (so anything that re-reads
        config later, including a future restart/``init()``, sees the new
        value) and, if the node already has a same-named plain instance
        attribute - the common pattern every node in ``nodes/`` uses,
        caching a config value as ``self.<name>`` in its own ``init()`` -
        keeps that copy in sync too, so code that reads ``self.<name>``
        picks up the change immediately. Whether a node's own
        ``process()`` loop reacts to that change *immediately* depends on
        whether it re-reads ``self.<name>`` on each pass or captured a
        local copy before entering its loop (many of the simpler nodes
        do the latter, so for those the new value takes effect on the
        node's next restart) - auditing all ~90 node types for this is
        outside what a generic mechanism here can promise, but the value
        is always applied and always visible going forward either way.
        """
        if name.startswith('_') or name in self._RESERVED_ATTRIBUTE_NAMES:
            logger.warning(
                "node %s (%s): refusing to set reserved/internal attribute %r via an attribute wire",
                self.id, type(self).__name__, name,
            )
            return
        self.config[name] = value
        if hasattr(self, name):
            setattr(self, name, value)
        self._last_items.append({'port': f'attr:{name}', 'item': value})
        if len(self._last_items) > self._max_last:
            self._last_items = self._last_items[-self._max_last:]

    def on_config_updated(self, changed_keys: set) -> None:
        """Extension hook: called after this node's config has been
        updated live (currently: PUT /nodes/{id}/config, the node
        editor's inline config panel), with the set of config keys that
        actually changed. Default no-op.

        Bug this closes (found live, from a direct report): a handful of
        node types - WebInputNode, ApiInputNode, ApiOutputNode,
        UserInputNode, WebOutputNode, WebOutputJSONNode - compute a
        derived value from their config exactly once in init() (typically
        an HTTP path/uri) and use it right then to register a route on
        the shared web server. set_attribute()/PUT /nodes/{id}/config
        updating self.config (and mirroring it onto a same-named
        self.<attr>, if one already exists) is *not* enough for any of
        these - the already-registered HTTP route still points at the
        *old* path, forever, no matter what the config now says. A user
        who creates an ApiInputNode, edits its `uri` field in the editor,
        and starts curling the *new* uri gets a plain 404 until they
        delete and recreate the node (or run the whole workflow, which
        instantiates a fresh node from the current config). Overriding
        this hook is how a node type re-derives that cached value and
        re-registers its route(s) - register_route()'s existing
        same-name replacement (see core/web_server.py) makes that safe to
        call again at any time, live route included.
        """

    def emit(self, name: str, item: Any):
        """Emit item to output port, update stats and last items buffer."""
        # record last items
        self._last_items.append({'port': name, 'item': item})
        if len(self._last_items) > self._max_last:
            self._last_items = self._last_items[-self._max_last:]
        # stats
        self._items_out += 1
        try:
            self._bytes_out += len(str(item).encode('utf-8'))
        except Exception:
            pass
        # Phase 1 fan-out change: broadcast to every live consumer pipe
        # currently under this port name, not just a single one - see
        # add_output()'s docstring. Snapshotted with list(...) so a
        # disconnect (remove_output()) racing with this loop can't mutate
        # the list out from under it mid-iteration.
        #
        # Phase 2 of the wire-kind-unification design (see
        # claude/design_unified_wire_kinds_plan.md): each consumer now
        # carries its own delivery kind. A "data" consumer gets `item`
        # exactly as before; a "raw" consumer gets it run through the same
        # unwrap helper an "attribute" edge already uses
        # (core/engine.py's _clean_attribute_value() - e.g. reducing a
        # ConstantValueNode's {'value': X, 'index': ...} down to the bare
        # X) - this replaces the old _auto_pair_raw_output()/separate
        # 'raw' port scheme, which just re-emitted the exact same,
        # un-unwrapped item on a second port. Imported lazily to avoid a
        # circular import (core.engine imports core.registry, which
        # imports every node module, which imports this one).
        pipes = self.outputs.get(name)
        if pipes:
            from .engine import _clean_attribute_value
            for pipe, kind in list(pipes):
                payload = _clean_attribute_value(item) if kind == 'raw' else item
                # Schedule the put, but keep a reference and a done-callback
                # so the task can't be garbage-collected mid-flight and so a
                # failure (e.g. a bounded Pipe that never drains) surfaces
                # in this node's health/last_error instead of only as an
                # untraceable "Task exception was never retrieved" warning.
                task = asyncio.create_task(pipe.put(payload))
                self._emit_tasks.add(task)
                task.add_done_callback(lambda t, port=name: self._on_emit_done(t, port))

    def record_output(self, entry: dict) -> None:
        """Record something this node actually did, for the live view
        (``GET /nodes/{id}/last``) and ``manual_emit()``, without going
        through ``emit()`` - for a node type with no real output port at
        all (a pure sink; see ``core/port_schema.py``'s ``[]``-output
        overrides), there is nothing to emit *to*, but a person watching
        the node's live view still deserves to see what it actually did
        with each item it received, instead of a live view that's
        permanently empty regardless of how much real work the node did.

        Feedback: "mqtt output node's live view is broken and is only
        showing '[]'" - ``MQTTOutputNode`` (like ``FileOutputNode``
        before it) publishes to a real broker but never called ``emit()``
        at all, so ``self._last_items`` never received anything from
        normal operation - the only entries that could ever land there
        were incidental attribute-wire-change records (``set_attribute()``
        appends ``{'port': f'attr:{name}', ...}``), which is consistent
        with a report of the live view intermittently showing *something*
        while otherwise reading empty. ``entry`` deliberately carries no
        ``'port'`` key (matching ``FileOutputNode``'s existing
        ``{'written': item}`` shape) so ``manual_emit()``'s replay loop -
        which only ever looks at entries with a string ``'port'`` - simply
        skips these, exactly as it should for a sink with nothing to
        replay onto.
        """
        self._last_items.append(entry)
        if len(self._last_items) > self._max_last:
            self._last_items = self._last_items[-self._max_last:]

    def manual_emit(self) -> dict[str, Any]:
        """Re-emit each output port's most recently emitted item again,
        right now, on demand - the node editor's "manual emit" title-bar
        button (and, for symmetry, the API's ``POST /nodes/{id}/emit`` and
        the MCP ``node_emit`` tool). Works identically for every node type
        with zero per-type customization: it just replays whatever
        ``emit()`` already recorded in ``self._last_items`` for each real
        output port, so a node that has never emitted anything yet (a
        fresh source with nothing to replay, or a sink with no output
        ports at all) simply has nothing to replay and returns ``{}``.

        This is deliberately a *replay*, not a re-run of ``process()`` -
        ``process()`` bodies vary wildly across the ~90 node types (many
        block on ``pipe.get()`` in an infinite loop with no notion of "one
        step"), so there's no generically safe way to force a single
        iteration of arbitrary node logic. Re-emitting the last known
        value per port is safe, instant, and useful the same way for every
        node: it re-triggers whatever is wired downstream without waiting
        for new data to arrive or a timer/trigger to fire again.

        Returns ``{port: item}`` for whatever was actually replayed, so a
        caller (the HTTP/MCP handlers) can report exactly what happened
        rather than just "ok". A port is reported here once it has ever
        been emitted to, whether or not anything is wired to it *right
        now* - deliberately not gated on `port in self.outputs`, since
        `emit()` itself already handles an unwired port safely (records
        the item, sends it nowhere) exactly the same way a real process()
        emit does, so replaying is consistent with that rather than a
        second, stricter notion of "emit" only this method enforces.
        """
        replayed: dict[str, Any] = {}
        # Walk newest-first so the *most recent* item on a given port wins
        # if that port appears more than once in the buffer (it always
        # will, for anything that's emitted more than once) - oldest
        # duplicates are skipped via the `port in replayed` check.
        for record in reversed(self._last_items):
            port = record.get('port')
            if not isinstance(port, str) or port.startswith('attr:') or port in replayed:
                continue
            item = record.get('item')
            self.emit(port, item)
            replayed[port] = item
        # Feedback: "user input node is not able to emit the prompt value
        # (no prior output)" - for a node that has never emitted on a
        # given port at all, there's nothing above to replay, which is a
        # real gap for any node type whose whole point is being manually
        # triggered (UserInputNode chief among them) rather than replaying
        # a real earlier emission. manual_emit_seed() is the per-type
        # escape hatch: the base implementation returns None for every
        # port (unchanged behavior - a port with nothing to replay and no
        # seed stays unreplayed), and a subclass can override it to supply
        # a sensible "first emit" value instead of a no-op. Checked only
        # for ports manual_emit_seed() has a real opinion about - not
        # every declared output port for every node type - so this can't
        # change behavior for the ~90 node types that don't override it.
        for port in self.outputs:
            if port in replayed:
                continue
            seed = self.manual_emit_seed(port)
            if seed is not None:
                self.emit(port, seed)
                replayed[port] = seed
        return replayed

    def manual_emit_seed(self, port: str) -> Any | None:
        """Override to give ``manual_emit()`` a value to send on ``port``
        when that port has never been emitted to (so there's nothing in
        ``self._last_items`` to replay). Returns ``None`` by default,
        meaning "no opinion" - such a port is simply left unreplayed,
        exactly like before this hook existed. See UserInputNode's
        override for the motivating case: its ``prompt`` config is a
        sensible value to send on a manual emit before anyone has ever
        actually submitted one.
        """
        return None

    def _on_emit_done(self, task: asyncio.Task, port: str):
        self._emit_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._error_count += 1
            self._last_error = str(exc)
            logger.error(
                "node %s (%s): emit('%s', ...) failed",
                self.id, type(self).__name__, port, exc_info=exc,
            )

    async def _control_listener(self, pipe: Pipe):
        while self._running or self._paused:
            try:
                msg = await asyncio.wait_for(pipe.get(), timeout=0.5)
                await self.handle_control(msg)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise
            except Exception:
                # Only this one source's listener breaks - a bad message
                # (or a handler bug) on one control wire must not silence
                # every other control source still wired to this node.
                logger.exception("node %s: control listener error", self.id)
                break

    async def handle_control(self, msg: Any):
        # default control handling: start/stop/pause/resume
        if isinstance(msg, str):
            cmd = msg.lower()
        elif isinstance(msg, dict):
            cmd = str(msg.get('action','')).lower()
        else:
            return
        if cmd == 'start':
            if not self._running:
                await self.start()
        elif cmd == 'stop':
            await self.stop()
        elif cmd == 'pause':
            await self.pause()
        elif cmd == 'resume':
            await self.resume()
        elif cmd in ('step', 'emit'):
            # Bug fix found while wiring up the new manual-emit feature:
            # POST /nodes/{id}/step (and the identical MCP node_step tool)
            # have sent a `{'action': 'step'}` control message since they
            # were first added, but this handler never had a 'step' case
            # at all - every "Step" click, on every node type, for as long
            # as that endpoint has existed, silently did nothing. Both
            # 'step' (the older, pre-existing action name) and 'emit' (the
            # new one) now do the same thing: manual_emit() below.
            self.manual_emit()
        elif cmd == 'reset':
            await self.reset()

    async def reset(self):
        """Clear this node's own accumulated state back to a fresh
        baseline, without stopping or restarting it - unlike stop()+
        start(), init()'s one-time setup (a compiled script, an opened
        connection/subprocess, ...) is left completely alone.

        The default here clears only the generic bookkeeping every node
        type accumulates: stats counters, the last-error/error-count
        fields, and the manual-emit replay buffer (``_last_items``). A
        node type that keeps its own accumulated state beyond that -
        ``StackNode``'s stack, ``TableNode``'s buffered rows, ... -
        overrides this, calls ``super().reset()``, and then clears its
        own state too (see those classes for examples) - the same
        override pattern ``manual_emit_seed()``/``health()`` already use
        elsewhere in this class.

        Wired up as ``{'action': 'reset'}`` through ``handle_control()``
        above - the same generic control-message mechanism ``'step'``/
        ``'emit'`` already use - so it's reachable from
        ``POST /nodes/{id}/reset``, the MCP ``reset_node`` tool, and the
        editor's right-click node menu. ``TriggerNode``'s own ``action``
        config field already passes any string straight through to its
        target unmodified, so wiring a ``TriggerNode`` with
        ``action: 'reset'`` at a target works with no changes to the
        Trigger node family at all.
        """
        self._error_count = 0
        self._last_error = None
        self._items_in = 0
        self._items_out = 0
        self._bytes_in = 0
        self._bytes_out = 0
        self._last_items = []
        if self._running:
            self._start_ts = time.monotonic()

    def get_last(self, n: int | None = None):
        if n is None:
            n = self._max_last
        return self._last_items[-n:]
