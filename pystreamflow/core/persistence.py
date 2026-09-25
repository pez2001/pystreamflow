import yaml
from .models import Graph, Node, Edge

def load_workflow(path: str) -> Graph:
    with open(path) as f:
        data = yaml.safe_load(f)
    g = Graph()
    g.version = data.get('version', 1)
    g.meta = data.get('meta', {})
    for n in data.get('nodes', []):
        g.add_node(Node(**n))
    for e in data.get('edges', []):
        g.add_edge(Edge(**e))
    _migrate_legacy_raw_edges(g)
    return g


def _migrate_legacy_raw_edges(graph: Graph) -> None:
    """Phase 3 of the wire-kind-unification design (see
    ``claude/design_unified_wire_kinds_plan.md``): before Phase 2, "raw"
    delivery meant wiring a plain ``data`` edge onto a separate,
    automatically-duplicated ``raw``/``raw_<name>`` output port
    (``core/port_schema.py``'s now-retired ``_add_raw_pairs()``/
    ``BaseNode``'s now-retired ``_auto_pair_raw_output()``). Now "raw" is a
    real edge *kind* on the node's one real output port instead, and that
    schema-level auto-pairing is gone - a workflow YAML saved under the old
    scheme would otherwise fail to load at all (``source_port: 'raw'`` no
    longer names a real port for most node types) or, worse, silently wire
    onto a port that doesn't exist and go nowhere.

    Rewrites each such edge **in place**, in memory, right after loading -
    before ``Engine.validate()``/``_wire_edges()`` ever see this graph, so
    both validate it against the *current* schema correctly: a
    ``source_port``/``type: "data"`` pair matching the old auto-pairing
    convention (``raw_port_name()``'s output) is rewritten to the real base
    port name with ``type: "raw"``, restoring the exact same delivery the
    old wire actually provided (see ``BaseNode.emit()``'s Phase 2 comment -
    the old auto-pair re-emitted the identical item on the second port,
    with no unwrapping; the new "raw" kind additionally unwraps via
    ``_clean_attribute_value()``, a deliberate refinement covered in the
    design doc rather than a literal behavior-preserving migration, since a
    node's own "raw" tap was already only ever fed the exact same item as
    its normal output - unwrapping it is the whole point of choosing "raw"
    going forward).

    Never touches an edge whose source type's output schema is ``DYNAMIC``
    (ForkNode's own numbered ``outN``/``rawN`` ports - a separate mechanism
    this design doesn't touch) or whose ``source_port`` is *already* a
    real, declared output of its type (e.g. ApiOutputNode's hand-declared
    ``raw`` port, which was never something the old auto-pairing
    generated) - see ``core/port_schema.py``'s ``legacy_raw_base_port()``
    docstring for the exact matching rule.
    """
    from .port_schema import DYNAMIC, get_port_schema, legacy_raw_base_port

    node_types = {n.id: n.type for n in graph.nodes}
    for e in graph.edges:
        if e.type != 'data':
            continue
        src_type = node_types.get(e.source)
        if not src_type:
            continue
        outs = get_port_schema(src_type)['outputs']
        if outs == DYNAMIC or e.source_port in outs:
            continue
        base = legacy_raw_base_port(e.source_port)
        if base and base in outs:
            e.source_port = base
            e.type = 'raw'

def save_workflow(graph: Graph, path: str):
    data = {
        'version': getattr(graph, 'version', 1),
        'meta': getattr(graph, 'meta', {}),
        'nodes': [vars(n) for n in graph.nodes],
        # `buffer` is optional per-edge queue config (media plan phase
        # 1.5); left out when unset so existing workflow files round-trip
        # unchanged.
        'edges': [
            {k: v for k, v in vars(e).items() if not (k == 'buffer' and v is None)}
            for e in graph.edges
        ]
    }
    with open(path, 'w') as f:
        yaml.dump(data, f)

def save_versioned(graph: Graph, base_path: str):
    import os, datetime
    os.makedirs(os.path.dirname(base_path) or '.', exist_ok=True)
    ts = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    version_path = base_path.replace('.yaml', f'_{ts}.yaml')
    save_workflow(graph, version_path)
    return version_path
