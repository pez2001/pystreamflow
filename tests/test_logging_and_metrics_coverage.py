"""
Coverage for pystreamflow/core/logging.py (previously 36% covered - only
the module import itself was exercised) and pystreamflow/core/metrics.py
(previously 62% covered - update_metrics()'s loop body and the /metrics
endpoint were never actually invoked).
"""
import json
import logging as std_logging

from pystreamflow.core import logging as psf_logging
from pystreamflow.core import metrics as psf_metrics
from pystreamflow.core.web_server import _nodes
from pystreamflow.nodes.clock_node import ClockNode


def test_simple_json_formatter_produces_valid_json():
    formatter = psf_logging.SimpleJsonFormatter()
    record = std_logging.LogRecord(
        name='test.logger', level=std_logging.INFO, pathname=__file__,
        lineno=1, msg='hello %s', args=('world',), exc_info=None,
    )
    formatted = formatter.format(record)
    parsed = json.loads(formatted)
    assert parsed['name'] == 'test.logger'
    assert parsed['level'] == 'INFO'
    assert parsed['message'] == 'hello world'
    assert 'asctime' in parsed


def test_setup_logging_installs_json_formatter_on_root_logger():
    root = std_logging.getLogger()
    original_handlers = root.handlers
    original_level = root.level
    try:
        psf_logging.setup_logging('DEBUG')
        assert root.level == std_logging.DEBUG
        assert len(root.handlers) == 1
        assert isinstance(root.handlers[0].formatter, psf_logging.SimpleJsonFormatter)
    finally:
        root.handlers = original_handlers
        root.setLevel(original_level)


def test_update_metrics_records_health_and_errors():
    node = ClockNode('metrics-node', {'interval': 0.02})
    _nodes[node.id] = node
    try:
        node._health = 'healthy'
        node._error_count = 3
        psf_metrics.update_metrics()
        assert psf_metrics.node_health.labels(node_id=node.id)._value.get() == 1.0
        assert psf_metrics.node_errors.labels(node_id=node.id)._value.get() == 3.0

        node._health = 'error'
        psf_metrics.update_metrics()
        assert psf_metrics.node_health.labels(node_id=node.id)._value.get() == 0.0
    finally:
        _nodes.pop(node.id, None)


def test_metrics_endpoint_returns_prometheus_text_and_status():
    # Bug fix (found during a codebase-wide audit for unimplemented/inert
    # functionality): metrics() used to return a Flask-style
    # `(body, status, headers)` tuple, which FastAPI has no idea what to do
    # with - it isn't a Response, so this used to only ever "work" when
    # called directly like a plain Python function (exactly what this test
    # used to do), never through a real mounted route. Now returns a real
    # Starlette Response.
    node = ClockNode('metrics-endpoint-node', {})
    _nodes[node.id] = node
    try:
        response = psf_metrics.metrics()
        assert response.status_code == 200
        assert response.headers['content-type'] == psf_metrics.CONTENT_TYPE_LATEST
        assert b'pystreamflow_node_health' in response.body
    finally:
        _nodes.pop(node.id, None)


def test_metrics_router_is_mounted_on_the_real_app():
    # The actual bug this whole pass found: `router` was built and unit-
    # tested (the two tests above) but never `include_router()`'d into any
    # app anywhere, so GET /metrics 404'd in every real deployment - only
    # calling metrics()/update_metrics() directly, as the tests above do,
    # ever exercised this module at all. Confirms it's reachable through
    # api/server.py's real app - the one pystreamflow_cli.py daemon
    # actually serves (see cli/main.py's `uvicorn.run(api_app, ...)`) - not
    # just importable.
    from fastapi.testclient import TestClient

    from conftest import TEST_API_KEY
    from pystreamflow.api.server import app as api_app

    node = ClockNode('metrics-mount-node', {})
    _nodes[node.id] = node
    try:
        client = TestClient(api_app, headers={"Authorization": f"Bearer {TEST_API_KEY}"})
        r = client.get('/metrics')
        assert r.status_code == 200
        assert r.headers['content-type'] == psf_metrics.CONTENT_TYPE_LATEST
        assert b'pystreamflow_node_health' in r.content
    finally:
        _nodes.pop(node.id, None)


def test_update_metrics_increments_counters_by_delta_not_cumulative_total():
    # Bug fix: node_errors/node_items_processed are Counters, meant to be
    # incremented by the *new* amount since the last scrape - the old code
    # called `.inc(h['error_count'])` with the node's whole running total
    # every single scrape, so calling update_metrics() twice in a row with
    # no new errors at all used to double the reported count anyway.
    node = ClockNode('metrics-delta-node', {'interval': 0.02})
    _nodes[node.id] = node
    try:
        node._health = 'healthy'
        node._error_count = 2
        psf_metrics.update_metrics()
        errors_after_first = psf_metrics.node_errors.labels(node_id=node.id)._value.get()

        # No new errors before the second scrape - the reported total must
        # not move at all.
        psf_metrics.update_metrics()
        assert psf_metrics.node_errors.labels(node_id=node.id)._value.get() == errors_after_first

        # A genuinely new error should add exactly its own delta, not the
        # whole new cumulative count on top of what was already reported.
        node._error_count = 5
        psf_metrics.update_metrics()
        assert psf_metrics.node_errors.labels(node_id=node.id)._value.get() == errors_after_first + 3
    finally:
        _nodes.pop(node.id, None)


def test_update_metrics_increments_items_processed():
    # Bug fix: node_items_processed was declared but never incremented
    # anywhere - Prometheus would report it frozen at 0 forever for every
    # node, real activity or not.
    node = ClockNode('metrics-items-node', {'interval': 0.02})
    _nodes[node.id] = node
    try:
        node._items_in = 0
        node._items_out = 4
        psf_metrics.update_metrics()
        assert psf_metrics.node_items_processed.labels(node_id=node.id)._value.get() == 4.0

        node._items_out = 10
        psf_metrics.update_metrics()
        assert psf_metrics.node_items_processed.labels(node_id=node.id)._value.get() == 10.0
    finally:
        _nodes.pop(node.id, None)
