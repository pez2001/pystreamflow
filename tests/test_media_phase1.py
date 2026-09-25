"""Media plan phase 1 (docs/plans/media_types_plan.md): MediaItem, the blob
store, BaseNode's binary-safe stats/history, JSON-safe live view,
GET /media/{ref}, and edge backpressure."""
import asyncio
import os
import uuid

import pytest
from fastapi.testclient import TestClient

from conftest import TEST_API_KEY
from pystreamflow.api.server import app
from pystreamflow.core import blob_store as blob_store_mod
from pystreamflow.core.blob_store import BlobStore, ensure_cleanup_task, set_blob_store, stop_cleanup_task
from pystreamflow.core.engine import Engine, _clean_attribute_value, edge_pipe, validate_buffer
from pystreamflow.core.media import MediaItem, item_size, sniff_mime, summarize_for_history, to_jsonable
from pystreamflow.core.models import Edge, Graph, Node
from pystreamflow.core.node import BaseNode
from pystreamflow.core.persistence import load_workflow, save_workflow
from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPEG = b"\xff\xd8\xff\xe0" + b"\x00" * 64


@pytest.fixture
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"), ttl_s=600, max_bytes=10 * 1024 * 1024)
    set_blob_store(s)
    yield s
    set_blob_store(None)


class SinkNode(BaseNode):
    async def process(self):
        pass


class ReprTrap(bytes):
    """bytes whose str()/repr() must never be built."""

    def __str__(self):
        raise AssertionError("str() of a binary payload was built")

    __repr__ = __str__


# --- sniffing -------------------------------------------------------------

@pytest.mark.parametrize("data,mime", [
    (PNG, "image/png"),
    (JPEG, "image/jpeg"),
    (b"GIF89a....", "image/gif"),
    (b"RIFF\x00\x00\x00\x00WEBPVP8 ", "image/webp"),
    (b"RIFF\x00\x00\x00\x00WAVEfmt ", "audio/wav"),
    (b"ID3\x04\x00\x00", "audio/mpeg"),
    (b"\xff\xfb\x90\x00", "audio/mpeg"),
    (b"OggS\x00\x02", "audio/ogg"),
    (b"fLaC\x00\x00", "audio/flac"),
    (b"\x00\x00\x00\x18ftypisom", "video/mp4"),
    (b"\x00\x00\x00\x18ftypM4A ", "audio/mp4"),
    (b"\x1a\x45\xdf\xa3\x9f\x42\x82\x84webm", "video/webm"),
    (b"hello world", None),
    (b"", None),
])
def test_sniff_mime(data, mime):
    assert sniff_mime(data) == mime


# --- MediaItem ------------------------------------------------------------

def test_media_item_inline_roundtrip(store):
    item = MediaItem.from_bytes(PNG, meta={"width": 1920, "height": 1080})
    assert item.kind == "image" and item.mime == "image/png"
    assert item.is_inline and item.ref is None
    assert item.get_bytes() == PNG
    assert item.size() == len(PNG)
    assert str(item) == f"<image/png 1920x1080 {len(PNG)}B>"
    summary = item.summary()
    assert summary["$media"] == "image" and summary["size"] == len(PNG)
    assert "data" not in summary
    assert store.stats()["count"] == 0


def test_media_item_ref_roundtrip(store):
    data = JPEG * 1000
    item = MediaItem.from_bytes(data, inline_limit=1024)
    assert not item.is_inline and item.data is None
    assert store.exists(item.ref)
    assert item.get_bytes() == data
    assert item.size() == len(data)
    assert asyncio.run(item.aget_bytes()) == data
    # a fresh handle with only the ref works too
    assert MediaItem(kind="image", mime="image/jpeg", ref=item.ref).size() == len(data)


def test_media_item_requires_payload_and_known_kind():
    with pytest.raises(ValueError):
        MediaItem(kind="image", mime="image/png")
    with pytest.raises(ValueError):
        MediaItem(kind="hologram", mime="x/y", data=b"1")


def test_media_item_defaults_for_unknown_bytes(store):
    item = MediaItem.from_bytes(b"just some bytes")
    assert item.kind == "binary" and item.mime == "application/octet-stream"


def test_media_item_with_data_keeps_meta(store):
    item = MediaItem.from_bytes(PNG, meta={"width": 10, "source_path": "a.png"})
    out = item.with_data(JPEG, mime="image/jpeg", meta={"width": 5})
    assert out.mime == "image/jpeg" and out.kind == "image"
    assert out.meta == {"width": 5, "source_path": "a.png"}
    assert item.meta["width"] == 10


def test_inline_limit_from_env(store, monkeypatch):
    monkeypatch.setenv("PSF_MEDIA_INLINE_MAX_MB", "0.001")  # ~1 KB
    assert not MediaItem.from_bytes(b"x" * 2000).is_inline
    assert MediaItem.from_bytes(b"x" * 500).is_inline


# --- blob store -----------------------------------------------------------

def test_blob_store_dedupes_by_content(store):
    a = store.put(b"same")
    b = store.put(b"same")
    assert a == b and store.stats()["count"] == 1


def test_blob_store_rejects_bad_refs(store):
    for bad in ("../../etc/passwd", "ABC", "0" * 63, ""):
        assert not store.exists(bad)
        with pytest.raises(KeyError):
            store.get(bad)


def test_blob_store_ttl_cleanup(store):
    ref = store.put(b"old")
    past = os.path.getmtime(store.path(ref)) - 3600
    os.utime(store.path(ref), (past, past))
    fresh = store.put(b"fresh")
    assert store.cleanup() == 1
    assert not store.exists(ref) and store.exists(fresh)
    with pytest.raises(KeyError):
        MediaItem(kind="binary", mime="application/octet-stream", ref=ref).get_bytes()


def test_blob_store_evicts_least_recently_used_over_limit(tmp_path):
    s = BlobStore(str(tmp_path), ttl_s=600, max_bytes=350)
    refs = []
    for i in range(3):
        refs.append(s.put(bytes([i]) * 100))
        t = 1_000_000 + i
        os.utime(s.path(refs[-1]), (t, t))
    # reading refs[0] makes it the most recently used
    s.get(refs[0])
    newest = s.put(b"\xff" * 100)
    assert s.exists(newest) and s.exists(refs[0])
    assert not s.exists(refs[1])
    assert s.exists(refs[2]) and s.stats()["bytes"] <= 350
    assert s.evicted >= 1


async def test_cleanup_task_runs_periodically(store, monkeypatch):
    monkeypatch.setenv("PSF_BLOB_CLEANUP_INTERVAL_S", "0.01")
    store.ttl_s = 0
    ref = store.put(b"short-lived")
    past = os.path.getmtime(store.path(ref)) - 10
    os.utime(store.path(ref), (past, past))
    await stop_cleanup_task()
    task = ensure_cleanup_task()
    assert ensure_cleanup_task() is task
    try:
        for _ in range(100):
            if not store.exists(ref):
                break
            await asyncio.sleep(0.01)
        assert not store.exists(ref)
    finally:
        await stop_cleanup_task()
    assert blob_store_mod._cleanup_task is None


# --- BaseNode ---------------------------------------------------------------

def test_emit_counts_binary_size_without_repr():
    node = SinkNode("n", {})
    payload = ReprTrap(b"\x00" * (50 * 1024 * 1024))
    node.emit("out", payload)
    assert node._bytes_out == len(payload)


def test_item_size():
    assert item_size(b"abc") == 3
    assert item_size(bytearray(5)) == 5
    assert item_size(memoryview(b"abcd")) == 4
    assert item_size("ä") == 2
    assert item_size({"a": 1}) == len(str({"a": 1}))


async def test_bytes_in_counts_binary_size_without_repr():
    node = SinkNode("n", {"auto_start": False})
    pipe = Pipe()
    node.add_input("in", pipe)
    payload = ReprTrap(b"\x01" * 1000)
    await pipe.put(payload)
    assert await node.inputs["in"].get() is payload
    assert node._bytes_in == 1000


def test_last_items_keep_only_summary_of_large_bytes_but_manual_emit_replays_real_payload():
    node = SinkNode("n", {"max_last_item_bytes": 100})
    big = b"\x89PNG" + b"\x00" * 1000
    node.emit("out", big)
    node.emit("out", {"value": big, "index": 1})
    hist = node.get_last()
    assert hist[0]["item"] == {"kind": "binary", "size": len(big), "head": big[:16].hex()}
    assert hist[1]["item"]["value"]["kind"] == "binary"
    assert hist[1]["item"]["index"] == 1
    node.emit("small", b"tiny")
    assert node.get_last()[-1]["item"] == b"tiny"

    got = []
    pipe = Pipe()
    node.add_output("out", pipe)

    async def run():
        replayed = node.manual_emit()
        await asyncio.sleep(0)
        got.append(await pipe.get())
        return replayed

    replayed = asyncio.run(run())
    assert replayed["out"] == {"value": big, "index": 1}
    assert got == [{"value": big, "index": 1}]


def test_large_inline_media_item_is_moved_to_blob_store_in_history(store):
    node = SinkNode("n", {"max_last_item_bytes": 100})
    item = MediaItem.from_bytes(PNG * 10)
    assert item.is_inline
    node.emit("out", item)
    kept = node.get_last()[-1]["item"]
    assert isinstance(kept, MediaItem) and kept.data is None and store.exists(kept.ref)
    assert node._last_real["out"] is item


def test_reset_clears_replay_buffer():
    node = SinkNode("n", {})
    node.emit("out", b"x")
    asyncio.run(node.reset())
    assert node._last_real == {} and node.manual_emit() == {}


def test_summarize_for_history_returns_same_object_when_nothing_changes():
    item = {"a": [1, 2, b"xy"], "b": "text"}
    assert summarize_for_history(item, 100) is item


async def test_emit_wait_blocks_on_full_block_edge():
    node = SinkNode("n", {})
    pipe = Pipe(maxsize=1, drop_policy="block")
    node.add_output("out", pipe)
    await node.emit_wait("out", 1)
    second = asyncio.create_task(node.emit_wait("out", 2))
    await asyncio.sleep(0.02)
    assert not second.done()
    assert await pipe.get() == 1
    await asyncio.wait_for(second, 1)
    assert await pipe.get() == 2
    assert node._items_out == 2


def test_clean_attribute_value_passes_media_item_through():
    item = MediaItem(kind="image", mime="image/png", data=PNG)
    assert _clean_attribute_value(item) is item


# --- JSON-safe serialization ------------------------------------------------

def test_to_jsonable(store):
    item = MediaItem.from_bytes(PNG, meta={"thumb": b"\x00\x01"})
    out = to_jsonable({"raw": b"\x89PNG\r\n", "items": (item,), 1: "x"})
    assert out["raw"] == {"$binary": 6, "head": "89504e470d0a"}
    media = out["items"][0]
    assert media["$media"] == "image" and media["mime"] == "image/png"
    assert media["meta"]["thumb"] == {"$binary": 2, "head": "0001"}
    assert media["preview_url"] == f"/media/{media['ref']}?mime=image/png"
    assert store.exists(media["ref"])
    assert out["1"] == "x"


def _client():
    return TestClient(app, headers={"Authorization": f"Bearer {TEST_API_KEY}"})


def test_node_last_with_binary_items_is_not_a_500(store):
    node_id = f"media-{uuid.uuid4().hex[:8]}"
    node = SinkNode(node_id, {})
    _nodes[node_id] = node
    try:
        node.emit("out", b"\xff\xfe\x00 not utf-8")
        node.emit("out", MediaItem.from_bytes(PNG))
        client = _client()
        for url in (f"/nodes/{node_id}/last", f"/reflection/nodes/{node_id}"):
            r = client.get(url)
            assert r.status_code == 200, r.text
        last = client.get(f"/nodes/{node_id}/last").json()["last"]
        assert last[0]["item"]["$binary"] == 13
        preview = last[1]["item"]["preview_url"]

        r = client.get(preview)
        assert r.status_code == 200
        assert r.content == PNG
        assert r.headers["content-type"] == "image/png"
        assert r.headers["x-content-type-options"] == "nosniff"

        r = client.post(f"/nodes/{node_id}/emit")
        assert r.status_code == 200
        assert r.json()["replayed"]["out"]["$media"] == "image"
    finally:
        _nodes.pop(node_id, None)


def test_media_endpoint_range_404_mime_and_auth(store):
    data = bytes(range(256)) * 4
    ref = store.put(data)
    client = _client()
    r = client.get(f"/media/{ref}", headers={"Range": "bytes=10-19"})
    assert r.status_code == 206 and r.content == data[10:20]
    # unknown bytes, no override -> octet-stream; unsafe override refused
    assert client.get(f"/media/{ref}").headers["content-type"] == "application/octet-stream"
    assert client.get(f"/media/{ref}?mime=text/html").headers["content-type"] == "application/octet-stream"
    assert client.get(f"/media/{ref}?mime=audio/wav").headers["content-type"] == "audio/wav"
    assert client.get("/media/" + "0" * 64).status_code == 404
    assert client.get("/media/..%2F..%2Fetc%2Fpasswd").status_code == 404
    assert TestClient(app).get(f"/media/{ref}").status_code == 401


def test_mcp_tool_results_are_json_safe(store):
    from pystreamflow.mcp.server import _execute_tool, _unwrap

    node_id = f"media-mcp-{uuid.uuid4().hex[:8]}"
    node = SinkNode(node_id, {})
    _nodes[node_id] = node
    try:
        node.emit("out", b"\x00\x01\x02")
        result = asyncio.run(_execute_tool("get_node_last", {"node_id": node_id}))
        assert _unwrap(result)["last"][0]["item"] == {"$binary": 3, "head": "000102"}
    finally:
        _nodes.pop(node_id, None)


# --- backpressure -----------------------------------------------------------

async def test_drop_policy_drops_immediately_without_timeout():
    p = Pipe(maxsize=2, drop_policy="drop")
    assert await p.put(1) and await p.put(2)
    assert await asyncio.wait_for(p.put(3), 0.5) is False
    assert p.stats()["dropped"] == 1
    assert [await p.get(), await p.get()] == [1, 2]


async def test_drop_oldest_keeps_latest_items():
    p = Pipe(maxsize=2, drop_policy="drop_oldest")
    for i in range(5):
        assert await p.put(i)
    assert p.dropped == 3
    assert [await p.get(), await p.get()] == [3, 4]


def test_unknown_drop_policy_is_rejected():
    with pytest.raises(ValueError):
        Pipe(drop_policy="sometimes")


def test_validate_buffer():
    assert validate_buffer(None) is None
    assert validate_buffer({"maxsize": 4, "drop_policy": "drop"}) is None
    assert validate_buffer({"maxsize": -1}) is not None
    assert validate_buffer({"maxsize": True}) is not None
    assert validate_buffer({"drop_policy": "nope"}) is not None
    assert validate_buffer({"size": 3}) is not None
    assert validate_buffer([1]) is not None


class FrameSource(BaseNode):
    MEDIA_OUTPUT_PORTS = {"frames": "drop_oldest", "file": "block"}

    async def process(self):
        pass


def test_edge_pipe_defaults(monkeypatch):
    monkeypatch.setenv("PSF_MEDIA_EDGE_MAXSIZE", "3")
    src = FrameSource("src", {})
    frames = edge_pipe(src, "frames")
    assert frames.queue.maxsize == 3 and frames.drop_policy == "drop_oldest"
    whole = edge_pipe(src, "file")
    assert whole.queue.maxsize == 3 and whole.drop_policy == "block"
    plain = edge_pipe(src, "out")
    assert plain.queue.maxsize == 0 and plain.drop_policy == "block"
    override = edge_pipe(src, "frames", {"maxsize": 1, "drop_policy": "drop"})
    assert override.queue.maxsize == 1 and override.drop_policy == "drop"


def _graph(buffer):
    g = Graph()
    g.add_node(Node(id="a", type="A", config={}))
    g.add_node(Node(id="b", type="B", config={}))
    g.add_edge(Edge(source="a", target="b", buffer=buffer))
    return g


async def test_engine_applies_edge_buffer_and_rejects_bad_one():
    eng = Engine(_graph({"maxsize": 4, "drop_policy": "drop"}))
    assert eng.validate()
    eng._instantiate_nodes()
    try:
        eng._wire_edges()
        pipe = eng.pipes[("a", "b")]
        assert pipe.queue.maxsize == 4 and pipe.drop_policy == "drop"
    finally:
        _nodes.pop("a", None)
        _nodes.pop("b", None)
    with pytest.raises(ValueError, match="drop_policy"):
        Engine(_graph({"drop_policy": "maybe"})).validate()


def test_workflow_buffer_roundtrip(tmp_path):
    path = str(tmp_path / "wf.yaml")
    save_workflow(_graph(None), path)
    assert "buffer" not in open(path).read()
    assert load_workflow(path).edges[0].buffer is None
    save_workflow(_graph({"maxsize": 8, "drop_policy": "drop_oldest"}), path)
    assert load_workflow(path).edges[0].buffer == {"maxsize": 8, "drop_policy": "drop_oldest"}


def test_connect_endpoint_accepts_buffer():
    client = _client()
    ids = [f"media-conn-{uuid.uuid4().hex[:8]}" for _ in range(2)]
    try:
        for nid in ids:
            r = client.post("/nodes", json={"node_id": nid, "node_type": "DisplayNode", "config": {}})
            assert "error" not in r.json(), r.json()
        req = {"source_id": ids[0], "source_port": "out", "target_id": ids[1], "target_port": "in"}
        r = client.post("/nodes/connect", json={**req, "buffer": {"drop_policy": "bogus"}})
        assert "drop_policy" in r.json().get("error", "")
        r = client.post("/nodes/connect", json={**req, "buffer": {"maxsize": 2, "drop_policy": "drop"}})
        assert r.json() == {"status": "connected"}
        pipe, _kind = _nodes[ids[0]].outputs["out"][-1]
        assert pipe.queue.maxsize == 2 and pipe.drop_policy == "drop"
    finally:
        for nid in ids:
            client.delete(f"/nodes/{nid}")
            _nodes.pop(nid, None)


def test_metrics_report_drops_and_blob_store(store):
    from pystreamflow.core import metrics

    node_id = f"media-metrics-{uuid.uuid4().hex[:8]}"
    node = SinkNode(node_id, {})
    pipe = Pipe(maxsize=1, drop_policy="drop")
    node.add_output("out", pipe)
    pipe.queue.put_nowait(0)
    pipe.dropped = 2
    store.put(b"blob")
    _nodes[node_id] = node
    try:
        metrics.update_metrics()
        assert metrics.pipe_dropped.labels(node_id=node_id)._value.get() == 2
        pipe.dropped = 5
        metrics.update_metrics()
        assert metrics.pipe_dropped.labels(node_id=node_id)._value.get() == 5
        assert metrics.blob_store_blobs._value.get() == 1
        assert metrics.blob_store_bytes._value.get() == 4
    finally:
        _nodes.pop(node_id, None)
        metrics.update_metrics()
