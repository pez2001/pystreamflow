"""Single source of truth for "node type name" -> "node class".

Before this module existed, three different places each built their own
copy of this mapping: Engine.run() had a ~90-entry dict written out by
hand, while api/server.py's ``/nodes`` endpoint and mcp/server.py each
independently looped over ``pystreamflow.nodes.__all__`` to build the same
thing on the fly. The hand-written copy in Engine had drifted out of sync
with the other two - ``UserInputNode`` was exported from the nodes package
and creatable through the API, but missing from Engine's dict, so any
workflow referencing it would silently fall back to a no-op node instead
of erroring.

Everything that needs to resolve a node-type name to a class should call
``build_node_registry()`` (or share a single instance of it) rather than
keeping its own copy.
"""
from __future__ import annotations


def build_node_registry() -> dict[str, type]:
    """Build the {type name: class} mapping from ``pystreamflow.nodes.__all__``.

    This is intentionally re-derived from the package's declared public
    API (``__all__``) rather than, say, ``dir(nodes_pkg)``, so anything
    exported there is automatically resolvable everywhere, and anything
    not meant to be a creatable node type (helpers, re-exported base
    classes, etc.) can be kept out simply by not adding it to ``__all__``.
    """
    from .. import nodes as nodes_pkg

    registry: dict[str, type] = {}
    for name in getattr(nodes_pkg, "__all__", []):
        cls = getattr(nodes_pkg, name, None)
        if cls is not None:
            registry[name] = cls
    return registry
