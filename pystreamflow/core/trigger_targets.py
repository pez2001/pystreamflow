"""Shared target-resolution logic for every trigger-style node
(``nodes/trigger.py``, ``nodes/trigger_advanced.py``).

Before this module existed, ten separate node classes across those two
files each hand-rolled a near-identical few lines: look up
``self.target_node_id`` (a plain string typed into node config) in the
process-wide live-node registry (``core.web_server._nodes``) and call
``handle_control`` on whatever it finds. That means the node editor's
"Trigger Wiring" mode - which draws a real graph edge of type ``control``
from a trigger node to its target - had no effect on triggering at all:
the edge was purely cosmetic, and the actual target was always whatever
ID happened to be typed into a JSON config field, wired or not (see the
project's evaluation doc, finding 4.3's last bullet: "Trigger targeting
bypasses the graph entirely").

``TriggerActionMixin`` fixes that: it resolves a trigger's target(s) from
a real ``control``-type edge in the graph first (populated onto the node
instance by ``Engine._wire_edges()`` as ``self._graph_control_targets``),
and only falls back to the hand-typed ``target_node_id`` config field when
no such edge exists - so config keeps working exactly as before for any
existing saved workflow, but drawing a control wire is now the primary,
graph-driven way to target a trigger, and multiple control edges from one
trigger node fire all of their targets.
"""
from __future__ import annotations

from .web_server import _nodes as _registered_nodes


class TriggerActionMixin:
    """Mix into a ``BaseNode`` subclass that has a ``target_node_id``
    attribute (set from config in the class's own ``init()``, as every
    trigger node already does) and, optionally, an ``action`` attribute
    used as the default action when none is passed explicitly.
    """

    def _trigger_targets(self) -> list[str]:
        graph_targets = getattr(self, "_graph_control_targets", None)
        if graph_targets:
            return list(graph_targets)
        target = getattr(self, "target_node_id", None)
        return [target] if target else []

    async def _trigger(self, action: str | None = None) -> None:
        act = action if action is not None else getattr(self, "action", "start")
        for target_id in self._trigger_targets():
            target = _registered_nodes.get(target_id)
            if target:
                await target.handle_control({"action": act})

    # trigger.py's classes historically called this one `_trigger_target()`
    # (no arguments) rather than `_trigger()` - kept as an alias so neither
    # file's call sites needed renaming.
    async def _trigger_target(self) -> None:
        await self._trigger()
