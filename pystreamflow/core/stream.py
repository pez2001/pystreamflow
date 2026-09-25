import asyncio
from typing import AsyncIterable, AsyncIterator

class Pipe:
    def __init__(self, maxsize: int = 0, drop_policy: str = 'block'):
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self.closed = False
        self.drop_policy = drop_policy  # block | drop | raise
        self.dropped = 0

    async def put(self, item, timeout: float | None = None):
        if self.closed:
            raise RuntimeError("Pipe closed")
        try:
            if timeout is not None:
                await asyncio.wait_for(self.queue.put(item), timeout=timeout)
            else:
                await self.queue.put(item)
        except asyncio.TimeoutError:
            if self.drop_policy == 'drop':
                self.dropped += 1
                return False
            elif self.drop_policy == 'raise':
                raise
            # else block forever
            await self.queue.put(item)
        return True

    async def get(self, timeout: float | None = None):
        if timeout is not None:
            return await asyncio.wait_for(self.queue.get(), timeout=timeout)
        return await self.queue.get()

    def get_nowait(self):
        """Non-blocking get; raises asyncio.QueueEmpty if nothing is queued.

        (Added because callers - e.g. SubgraphNode - expected Pipe to have
        this like asyncio.Queue does; it was previously missing entirely,
        so any such call raised AttributeError instead of the QueueEmpty
        the caller was already prepared to catch.)
        """
        return self.queue.get_nowait()

    async def close(self):
        self.closed = True
        # unblock consumers
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break

    def stats(self):
        return {"size": self.queue.qsize(), "dropped": self.dropped, "closed": self.closed}

class Fork:
    """Fan a single Pipe out to multiple branch Pipes.

    Not currently wired into ForkNode (nodes/modifier_fork.py has its own
    inline polling loop instead) - kept as a reusable primitive for that
    kind of node to be rebuilt on top of later, per the code-reuse phase
    of the pystreamflow refactor plan.
    """
    def __init__(self, source: Pipe):
        self.source = source
        self.branches: list[Pipe] = []

    def add_branch(self) -> Pipe:
        p = Pipe()
        self.branches.append(p)
        return p

    async def run(self):
        while not self.source.closed:
            try:
                item = await asyncio.wait_for(self.source.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            for b in self.branches:
                await b.put(item)

class Merge:
    """Merge multiple source Pipes into a single output Pipe.

    Not currently wired into MergeNode (nodes/modifier_merge.py has its
    own inline round-robin loop instead) - see the ``Fork`` docstring
    above; kept as a reusable primitive for later.
    """
    def __init__(self):
        self.sources: list[Pipe] = []
        self.output = Pipe()

    def add_source(self, pipe: Pipe):
        self.sources.append(pipe)

    async def run(self):
        # One get()-task per source, tracked by index so a completed task
        # can be re-armed against *its own* source pipe. The previous
        # version re-armed with `asyncio.create_task(d.get())` where `d`
        # was the just-completed Task itself rather than the source Pipe -
        # asyncio.Task has no .get() method, so this raised AttributeError
        # the instant any item arrived.
        tasks = {i: asyncio.create_task(s.get()) for i, s in enumerate(self.sources)}
        try:
            while tasks:
                done, _ = await asyncio.wait(tasks.values(), return_when=asyncio.FIRST_COMPLETED)
                for i, t in list(tasks.items()):
                    if t not in done:
                        continue
                    source = self.sources[i]
                    if t.cancelled():
                        del tasks[i]
                        continue
                    exc = t.exception()
                    if exc is not None:
                        del tasks[i]
                        continue
                    await self.output.put(t.result())
                    if source.closed:
                        del tasks[i]
                    else:
                        tasks[i] = asyncio.create_task(source.get())
        finally:
            for t in tasks.values():
                t.cancel()


class FanInPipe:
    """Merges an arbitrary, dynamically-changing number of source Pipes
    into one logical Pipe-shaped endpoint, exposing the same
    ``get()``/``put()``/``stats()`` surface a plain ``Pipe`` does so it
    can be dropped in anywhere a single pipe is expected.

    This is the input-side counterpart to the fan-out fix already applied
    to ``BaseNode.outputs`` and the fan-in fix already applied to
    ``BaseNode._control_pipes`` (see ``core/node.py``): a node's regular
    *data* input port used to be a single overwritable slot too
    (``self.inputs[name] = ...``), so a second wire landing on the same
    named input silently stole the connection from whichever source was
    wired first, with no error - the exact same bug class, just never
    fixed on this third side of it. ``BaseNode.add_input()`` reaches for
    this class the moment a second source shows up for one input name;
    see its own docstring for the full wiring story.

    Differs from ``Merge`` above in the ways that actually matter for
    this job: sources can be added/removed live rather than fixed at
    construction (``Merge.run()`` was never actually wired into anything
    live - see its own docstring), a source's own in-flight ``get()``
    task is kept and reused across repeated ``get()`` calls on this
    object instead of being torn down and recreated each time (so a
    caller wrapping ``get()`` in ``asyncio.wait_for(..., timeout=...)`` -
    a pattern several node types' ``process()`` loops use - loses nothing
    when that outer timeout fires and retries: the underlying per-source
    task just stays parked, waiting for the next real item, exactly as if
    nothing had happened), and ``put()`` is supported directly on this
    object (queued through an always-armed internal "manual" source) so a
    direct injection - e.g. the HTTP/MCP ``send_to_node`` tool - keeps
    working exactly as it did against a single un-merged pipe.

    Once a ``FanInPipe`` is created for an input name it's always backing
    at least two real sources for its whole life; ``BaseNode`` tears the
    wrapper back down and reverts to a plain, un-merged pipe the moment a
    disconnect brings a name back down to exactly one source (see
    ``remove_input()``), so the overwhelmingly common single-source case
    never pays for any of this.
    """

    def __init__(self):
        self._sources: list[Pipe] = []
        self._manual: asyncio.Queue = asyncio.Queue()
        self._pending: dict[int, asyncio.Task] = {}
        self._buffer: list = []

    def add_source(self, pipe: Pipe) -> None:
        self._sources.append(pipe)

    def remove_source(self, pipe: Pipe) -> bool:
        """Remove exactly one source, cancelling its in-flight get() task
        (if any) so it doesn't linger forever waiting on a pipe nothing
        will ever read from again. Returns False if ``pipe`` wasn't (or
        is no longer) one of this instance's sources.
        """
        try:
            self._sources.remove(pipe)
        except ValueError:
            return False
        task = self._pending.pop(id(pipe), None)
        if task is not None and not task.done():
            task.cancel()
        return True

    async def put(self, item, timeout: float | None = None):
        """Direct injection (e.g. ``send_to_node``), delivered through the
        always-armed internal manual queue alongside every wired source -
        so it's picked up by the very next ``get()`` exactly like an item
        arriving from a real wire would be, and existing callers that
        expect ``put()`` to work on any wired input pipe keep working
        unchanged even once that input has more than one source.
        """
        if timeout is not None:
            await asyncio.wait_for(self._manual.put(item), timeout=timeout)
        else:
            await self._manual.put(item)
        return True

    def _arm(self) -> None:
        # (Re-)arm a get() task for the manual queue and for every
        # currently-wired source that doesn't already have one in flight.
        # Only replaces a slot that's genuinely empty (``None``) - a task
        # already sitting there, done or not, is left alone: _drain_done()
        # below is what collects a finished one's result, and creating a
        # fresh task over top of it (as an earlier version of this method
        # did, keyed only on `.done()`) would silently throw that result
        # away. Reusing rather than replacing a still-*pending* task is
        # also what makes an outer asyncio.wait_for(...) timeout-and-retry
        # around get() safe (see the class docstring).
        for key, source in [(id(self._manual), self._manual)] + [(id(s), s) for s in self._sources]:
            if key not in self._pending:
                self._pending[key] = asyncio.create_task(source.get())

    def _drain_done(self) -> list:
        """Collect the results of every currently-pending task that has
        already finished, removing each from ``self._pending``.

        Needed alongside ``_arm()``'s "don't replace a pending slot"
        contract above: a per-source task can finish on its own, with
        nothing actively watching it, whenever a *previous* ``get()`` call
        was itself cancelled from the outside (the exact
        ``asyncio.wait_for(fp.get(), timeout=...)`` pattern several node
        types' ``process()`` loops use) while that task kept running in
        the background - ``asyncio.wait()`` stops watching its arguments
        on cancellation, it doesn't cancel them. Without this drain step,
        the next ``get()`` call's own ``asyncio.wait()`` would just wait
        on that already-finished task again, which resolves immediately
        but was never actually collected into ``results`` - probing this
        was extracted into its own step, called both before arming (in
        case something finished while nobody was watching at all) and
        after every ``asyncio.wait()`` round.
        """
        results = []
        for key, task in list(self._pending.items()):
            if not task.done():
                continue
            del self._pending[key]
            if task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                raise exc
            results.append(task.result())
        return results

    async def get(self):
        if self._buffer:
            return self._buffer.pop(0)
        while True:
            already_done = self._drain_done()
            if already_done:
                self._buffer.extend(already_done[1:])
                return already_done[0]
            self._arm()
            # The return value is unused - _drain_done() above already
            # inspects `.done()` on each task directly rather than relying
            # on this call's own done/pending sets, so this is here purely
            # to block until at least one task finishes.
            await asyncio.wait(list(self._pending.values()), return_when=asyncio.FIRST_COMPLETED)
            results = self._drain_done()
            if results:
                # More than one source can resolve in the same event-loop
                # tick (e.g. two items arriving back to back) - return the
                # first, keep the rest buffered for the next get() call(s)
                # rather than dropping them.
                self._buffer.extend(results[1:])
                return results[0]
            # Everything that completed this round was cancelled (e.g. a
            # source was removed the instant its get() resolved) - loop
            # around and re-arm rather than returning nothing.

    def stats(self) -> dict:
        size = len(self._buffer) + self._manual.qsize()
        dropped = 0
        for s in self._sources:
            try:
                st = s.stats()
                size += st.get('size', 0)
                dropped += st.get('dropped', 0)
            except Exception:
                pass
        # Not counted above: an item already dequeued out of a source's
        # own queue into one of self._pending's in-flight get() tasks but
        # not yet delivered to a caller - a narrow, transient undercount
        # acceptable for an observability field, not a correctness one.
        return {"size": size, "dropped": dropped, "closed": False, "fan_in": len(self._sources)}
