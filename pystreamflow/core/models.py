from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

@dataclass
class Node:
    id: str
    type: str
    config: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Edge:
    source: str
    target: str
    source_port: str = "out"
    target_port: str = "in"
    # "data" | "control" | "endpoint" | "attribute" - matches the wiring kind
    # chosen in the node editor. Kept optional/defaulted so existing workflow
    # YAML files (saved before this field existed) still load unchanged.
    type: str = "data"
    # Optional per-edge queue settings, e.g. ``{maxsize: 8, drop_policy:
    # drop_oldest}`` - see core/stream.py's Pipe for the policies and
    # core/engine.py's edge_pipe() for the defaults of media edges.
    buffer: Optional[Dict[str, Any]] = None

@dataclass
class Graph:
    nodes: List[Node] = field(default_factory=list)
    edges: List[Edge] = field(default_factory=list)

    def add_node(self, node: Node):
        self.nodes.append(node)

    def add_edge(self, edge: Edge):
        self.edges.append(edge)
