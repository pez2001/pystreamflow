"""
Simple Prometheus metrics exporter for PyStreamFlow nodes.

Bug fixes (found during a codebase-wide audit for unimplemented/inert
functionality - this whole module was built and unit-tested in isolation
but never actually wired into a real deployment):

1. ``router`` was never ``include_router()``'d into any app anywhere - not
   ``api/server.py``'s ``app`` (the one actually served by
   ``pystreamflow_cli.py daemon`` - see its own ``uvicorn.run(api_app, ...)``
   call) and not ``core/web_server.py``'s ``app`` either. So ``GET
   /metrics`` 404'd in every real deployment; this module only ever ran
   under its own tests, which call ``metrics()``/``update_metrics()``
   directly rather than through a mounted route. Fixed by
   ``api/server.py`` now doing ``app.include_router(router)`` alongside its
   other mounts.
2. ``metrics()`` returned a Flask-style ``(body, status, headers)`` tuple.
   FastAPI has no idea what to do with that - a plain tuple return value
   gets run through FastAPI's default JSON response handling, which would
   try to serialize ``generate_latest()``'s raw ``bytes`` (and fail, or at
   best mangle it into a JSON array of byte values) rather than actually
   sending Prometheus's real text-exposition format with the right
   Content-Type. Fixed by returning a real Starlette ``Response`` with the
   raw bytes and ``CONTENT_TYPE_LATEST`` as ``media_type``, which is the
   documented way to serve ``prometheus_client`` output from FastAPI.
3. ``node_errors``/``node_items_processed`` are Prometheus ``Counter``s,
   which only make sense monotonically increasing by the *new* amount
   since the last scrape - but ``update_metrics()`` called
   ``node_errors.labels(...).inc(h['error_count'])`` with the node's whole
   *cumulative* error count every single scrape, so every repeated ``GET
   /metrics`` kept re-adding the same already-counted errors on top of the
   running total (a healthy node with 1 real error would report an
   ever-growing error count with every scrape, not a steady 1).
   ``node_items_processed`` was declared but never incremented at all -
   Prometheus would just report it frozen at 0 forever. Both are fixed the
   same way: track each node's last-seen cumulative value and ``.inc()``
   only the delta since the previous scrape, the standard pattern for
   deriving a Counter from an already-cumulative source value.
"""
from fastapi import APIRouter, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

from .blob_store import get_blob_store
from .web_server import _nodes

router = APIRouter()

node_health = Gauge('pystreamflow_node_health', 'Node health 1=healthy', ['node_id'])
# "Items processed" = items_in + items_out from BaseNode.health()['stats'] -
# the sum (rather than either alone) is the one definition that's
# meaningful for every node shape: a pure source node's items_in is always
# 0 (nothing to consume), a pure sink's items_out is always 0 (nothing to
# produce), so either count alone would sit frozen at 0 forever for a
# whole class of real, working node types.
node_items_processed = Counter('pystreamflow_node_items_processed_total', 'Items processed', ['node_id'])
node_errors = Counter('pystreamflow_node_errors_total', 'Errors', ['node_id'])

# Per-node last-seen cumulative (error_count, items_in + items_out), so
# update_metrics() can turn BaseNode.health()'s running totals into the
# *delta*-per-scrape a Prometheus Counter is actually meant to receive -
# see this module's own docstring, point 3, for the double-counting bug
# this closes.
_last_seen: dict[str, tuple[int, int]] = {}

# Media plan phase 1.5/1.2: items dropped by bounded edges (summed over
# every consumer pipe of a node's output ports, attributed to the
# producing node) and the media blob store's footprint.
pipe_dropped = Counter('pystreamflow_pipe_dropped_total', 'Items dropped by full bounded edges', ['node_id'])
blob_store_bytes = Gauge('pystreamflow_blob_store_bytes', 'Bytes held in the media blob store')
blob_store_blobs = Gauge('pystreamflow_blob_store_blobs', 'Blobs held in the media blob store')
_last_dropped: dict[str, int] = {}


def _output_dropped(node) -> int:
    total = 0
    for pipes in getattr(node, 'outputs', {}).values():
        for pipe, _kind in pipes:
            try:
                total += int(pipe.stats().get('dropped', 0))
            except Exception:
                pass
    return total


def update_metrics():
    live_ids = set(_nodes.keys())
    for nid, node in _nodes.items():
        h = node.health()
        node_health.labels(node_id=nid).set(1 if h['health'] == 'healthy' else 0)

        prev_errors, prev_items = _last_seen.get(nid, (0, 0))

        error_count = h['error_count']
        error_delta = error_count - prev_errors
        if error_delta > 0:
            node_errors.labels(node_id=nid).inc(error_delta)

        items_total = h['stats']['items_in'] + h['stats']['items_out']
        items_delta = items_total - prev_items
        if items_delta > 0:
            node_items_processed.labels(node_id=nid).inc(items_delta)

        _last_seen[nid] = (error_count, items_total)

        dropped = _output_dropped(node)
        dropped_delta = dropped - _last_dropped.get(nid, 0)
        if dropped_delta > 0:
            pipe_dropped.labels(node_id=nid).inc(dropped_delta)
        _last_dropped[nid] = dropped

    # Drop bookkeeping for nodes that no longer exist (deleted, or a
    # from a previous session) so this dict doesn't grow without bound
    # across a long-running daemon's whole lifetime.
    for stale_id in set(_last_seen) - live_ids:
        del _last_seen[stale_id]
    for stale_id in set(_last_dropped) - live_ids:
        del _last_dropped[stale_id]

    try:
        st = get_blob_store().stats()
        blob_store_bytes.set(st['bytes'])
        blob_store_blobs.set(st['count'])
    except Exception:
        pass


@router.get('/metrics')
def metrics():
    update_metrics()
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
