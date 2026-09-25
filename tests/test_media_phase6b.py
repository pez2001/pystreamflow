"""Media plan phase 6b (docs/plans/media_types_plan.md): advisory port data
types - schema, warnings on every wiring surface, and their consistency
with the real port schema."""
import asyncio
import logging
import uuid

import pytest
from fastapi.testclient import TestClient

from conftest import TEST_API_KEY
from pystreamflow.api.server import app
from pystreamflow.core.engine import Engine
from pystreamflow.core.models import Edge, Graph, Node
from pystreamflow.core.port_schema import (
    DTYPES, DYNAMIC, _PORT_DTYPES, all_port_dtypes, dtype_warning, edge_warning, get_port_dtypes,
    get_port_schema, port_dtype,
)
from pystreamflow.core.registry import build_node_registry
from pystreamflow.core.web_server import _nodes

client = TestClient(app, headers={"Authorization": f"Bearer {TEST_API_KEY}"})


@pytest.mark.parametrize("src,tgt,ok", [
    ("any", "image", True), ("image", "any", True), ("image", "image", True),
    ("image", "media", True), ("media", "audio", True),
    ("image", "audio", False), ("video", "image", False),
    ("image", "text", False), ("text", "image", False), ("json", "video", False),
    ("number", "text", True), ("json", "text", True), ("number", "json", True),
    ("text", "number", False), ("json", "number", False), ("text", "json", False),
    (None, "number", True),
])
def test_dtype_warning_matrix(src, tgt, ok):
    assert (dtype_warning(src, tgt) is None) == ok


def test_warning_text_is_readable():
    assert dtype_warning("video", "image") == "video output into an image input - the target will pass it through or fail"
    assert "a text input" in dtype_warning("audio", "text")


def test_declared_dtypes_match_real_ports():
    registry = build_node_registry()
    for type_name, (ins, outs) in _PORT_DTYPES.items():
        schema = get_port_schema(type_name)
        for port, dtype in ins.items():
            assert dtype in DTYPES
            assert schema["inputs"] == DYNAMIC or port in schema["inputs"], (type_name, port)
        for port, dtype in outs.items():
            assert dtype in DTYPES
            assert schema["outputs"] == DYNAMIC or port in schema["outputs"], (type_name, port)
    # every registered type that declares dtypes is reported
    assert set(all_port_dtypes()) == {t for t in _PORT_DTYPES if t in registry}


def test_port_dtype_lookup_and_defaults():
    assert port_dtype("ImageResizeNode", "in", "inputs") == "image"
    assert port_dtype("ImageResizeNode", "control", "inputs") == "any"
    assert port_dtype("NoSuchNode", "out", "outputs") == "any"
    assert get_port_dtypes("AudioLevelNode")["outputs"] == {"out": "json", "rms_db": "number", "peak_db": "number"}
    assert get_port_dtypes("VideoDecodeNode") == {"inputs": {"in": "video"}, "outputs": {"out": "image", "audio": "audio"}}


def test_edge_warning():
    assert edge_warning("VideoDecodeNode", "out", "TextUpperNode", "in").startswith(
        "VideoDecodeNode.out -> TextUpperNode.in: image output into a text input")
    assert edge_warning("VideoDecodeNode", "out", "ImageResizeNode", "in") is None
    assert edge_warning("MediaFileInputNode", "out", "AudioDecodeNode", "in") is None
    assert edge_warning("AudioLevelNode", "rms_db", "NumericRoundNode", "in") is None
    assert edge_warning("VideoDecodeNode", "audio", "ImageResizeNode", "in", "raw") is not None
    for kind in ("control", "attribute"):
        assert edge_warning("VideoDecodeNode", "out", "TextUpperNode", "in", kind) is None


def test_node_schema_shape_is_unchanged():
    schema = client.get("/node-schema").json()
    assert schema["ImageResizeNode"] == {"inputs": ["in"], "outputs": ["out"]}
    assert all(set(v) == {"inputs", "outputs"} for v in schema.values())


def test_port_dtypes_endpoint():
    body = client.get("/port-dtypes").json()
    assert body["dtypes"] == list(DTYPES)
    assert body["types"]["VideoEncodeNode"] == {"inputs": {"in": "image", "audio": "audio"}, "outputs": {"out": "video"}}
    assert "LogOutputNode" not in body["types"]


def test_connect_endpoint_warns_but_connects():
    ids = {k: f"dt-{k}-{uuid.uuid4().hex[:6]}" for k in ("thumb", "text", "resize")}
    try:
        for key, node_type in (("thumb", "VideoThumbnailNode"), ("text", "TextUpperNode"), ("resize", "ImageResizeNode")):
            r = client.post("/nodes", json={"node_id": ids[key], "node_type": node_type,
                                            "config": {"max_side": 10} if key == "resize" else {}})
            assert "error" not in r.json(), r.json()
        r = client.post("/nodes/connect", json={"source_id": ids["thumb"], "source_port": "out",
                                                "target_id": ids["text"], "target_port": "in"})
        body = r.json()
        assert body["status"] == "connected" and "image output into a text input" in body["warning"]
        assert _nodes[ids["thumb"]].outputs["out"]
        r = client.post("/nodes/connect", json={"source_id": ids["thumb"], "source_port": "out",
                                                "target_id": ids["resize"], "target_port": "in"})
        assert r.json() == {"status": "connected"}
    finally:
        for nid in ids.values():
            client.delete(f"/nodes/{nid}")
            _nodes.pop(nid, None)


def test_workflow_save_reports_warnings():
    wf = {"nodes": [{"id": "a", "type": "AudioLevelNode"}, {"id": "b", "type": "ImageResizeNode"},
                    {"id": "c", "type": "NumericRoundNode"}],
          "edges": [{"source": "a", "target": "b"},
                    {"source": "a", "source_port": "peak_db", "target": "c"}]}
    body = client.post("/workflows", json=wf).json()
    assert body["status"] == "saved"
    assert len(body["warnings"]) == 1 and body["warnings"][0].startswith("AudioLevelNode.out -> ImageResizeNode.in")
    clean = client.post("/workflows", json={"nodes": wf["nodes"], "edges": wf["edges"][1:]}).json()
    assert "warnings" not in clean


def test_engine_logs_dtype_warnings(caplog):
    g = Graph()
    g.add_node(Node(id="img", type="ImageThumbnailNode"))
    g.add_node(Node(id="txt", type="TextUpperNode"))
    g.add_edge(Edge(source="img", target="txt"))
    with caplog.at_level(logging.WARNING, logger="pystreamflow.engine"):
        assert Engine(g).validate() is True
    assert any("image output into a text input" in r.getMessage() for r in caplog.records)


def test_mcp_connect_reports_warning():
    from pystreamflow.mcp.server import _execute_tool, _unwrap

    ids = [f"mcp-dt-{uuid.uuid4().hex[:6]}" for _ in range(2)]
    try:
        for nid, node_type in zip(ids, ("VideoThumbnailNode", "NumericAddNode")):
            assert "error" not in asyncio.run(_execute_tool("create_node", {"node_id": nid, "node_type": node_type}))
        result = _unwrap(asyncio.run(_execute_tool("connect_nodes", {
            "source_id": ids[0], "source_port": "out", "target_id": ids[1], "target_port": "in"})))
        assert result["status"] == "connected" and "image output into a number input" in result["warning"]
    finally:
        for nid in ids:
            asyncio.run(_execute_tool("delete_node", {"node_id": nid}))
            _nodes.pop(nid, None)
