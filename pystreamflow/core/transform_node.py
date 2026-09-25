"""Shared ``process()``-loop base class for the numeric_* and text_*
node families (evaluation doc finding 4.2).

Before this module, all ~11 ``numeric_*`` and ~10 ``text_*`` node types
were separate files, each hand-copying the exact same loop: read one item
from the ``in`` pipe with a 1-second timeout, apply a transform, emit the
result on ``out`` - or, if the transform raised (a non-numeric string fed
into ``NumericAddNode``, say), emit the *original* item unchanged instead
of crashing the node or dropping it. The only things that actually
differed between e.g. ``NumericAddNode`` and ``NumericSubNode``, or
``TextUpperNode`` and ``TextReverseNode``, were which one or two config
values got parsed in ``init()`` (if any) and the one-line transform
itself. That meant any fix to the shared loop - the idle-when-unwired
behavior, the timeout handling, the "never crash on bad input" error
swallowing - had to be copy-pasted into ~21 files to actually take, and
had already drifted at least once (``numeric_round.py`` imported ``math``
and never used it).

``SingleInputTransformNode`` is that shared loop, written once. A
concrete node now only needs to provide the one thing that's genuinely
specific to it: ``transform(item)`` for the per-item computation, and
optionally ``configure()`` for one-time config parsing (called from
``init()``, so subclasses don't need to override ``init()`` - or remember
to call ``super().init()`` - just to read a config value or two).
"""
from __future__ import annotations

import asyncio
from typing import Any

from .node import BaseNode


class SingleInputTransformNode(BaseNode):
    """BaseNode subclass for "read one item from a single ``in`` port,
    transform it, emit the result on ``out``" - the common shape shared
    by every ``numeric_*`` and ``text_*`` node type.

    Subclasses override:

    - ``transform(self, item) -> Any`` (required) - the actual per-item
      computation. If this raises for a given item, the *original* item
      is emitted unchanged rather than the node crashing or silently
      dropping it - matching every node in both families' pre-existing
      behavior exactly (e.g. ``NumericAddNode`` passing through a
      non-numeric string it can't add to, instead of stopping the whole
      downstream pipeline over one bad item).
    - ``async def configure(self)`` (optional) - one-time config parsing,
      called once from ``init()`` before the process loop starts. No-op
      by default, for the several node types (``NumericAbsNode``,
      ``TextUpperNode``, ...) that take no config at all.
    """

    async def configure(self) -> None:
        return None

    async def init(self):
        await self.configure()

    def transform(self, item: Any) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} must implement transform(item)",
        )

    async def process(self):
        # `pipe` used to be fetched once, before this loop started; if the
        # 'in' port wasn't wired at that exact instant, the whole method
        # would sleep once and then `return` - ending the node's process
        # task for good (BaseNode._run_loop treats a normal return as
        # terminal). Under normal Engine-driven runs this was low-risk
        # since Engine._wire_edges() wires every input before starting any
        # node, but any node started before its 'in' edge exists (a node
        # added to a live graph, or driven directly/standalone) would
        # never process anything even after being wired moments later.
        # Re-checking self.inputs.get('in') on every pass instead makes
        # this, and every numeric_*/text_* node built on it, resilient to
        # being wired after it starts.
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                try:
                    result = self.transform(item)
                    self.emit('out', result)
                except Exception:
                    self.emit('out', item)
            except asyncio.TimeoutError:
                await asyncio.sleep(0.1)
