"""Media plan phase 2 (docs/plans/media_types_plan.md): media file I/O,
uploads into WebInputNode/ApiInputNode, serving media from
WebOutputNode/ApiOutputNode, and media-aware Base64 nodes."""
import asyncio
import base64
import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from pystreamflow.core.blob_store import BlobStore, set_blob_store
from pystreamflow.core.config_schema import get_field_schema
from pystreamflow.core.media import MediaItem, extension_for_mime, guess_mime, is_media_filename
from pystreamflow.core.port_schema import get_port_schema
from pystreamflow.core.registry import build_node_registry
from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes, app, http_endpoint_info
from pystreamflow.nodes.base64_decode_node import Base64DecodeNode
from pystreamflow.nodes.base64_encode_node import Base64EncodeNode
from pystreamflow.nodes.input_api import ApiInputNode
from pystreamflow.nodes.input_directory import DirectoryInputNode
from pystreamflow.nodes.input_web import WebInputNode
from pystreamflow.nodes.media_file_input import MediaFileInputNode
from pystreamflow.nodes.media_file_output import MediaFileOutputNode
from pystreamflow.nodes.output_api import ApiOutputNode
from pystreamflow.nodes.web_output import WebOutputNode
from pystreamflow.nodes.web_output_json import WebOutputJSONNode

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
JPEG = b"\xff\xd8\xff\xe0" + b"\x01" * 64
WAV = b"RIFF\x00\x00\x00\x00WAVEfmt " + b"\x00" * 64

client = TestClient(app)


@pytest.fixture(autouse=True)
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"))
    set_blob_store(s)
    yield s
    set_blob_store(None)


def _uid(prefix):
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


async def _wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return cond()


def _emitted(node, port="out"):
    return [e["item"] for e in node.get_last() if e.get("port") == port]


# --- helpers in core/media.py ------------------------------------------------

def test_mime_helpers():
    assert guess_mime(PNG, "x.jpg") == "image/png"  # magic bytes win
    assert guess_mime(b"no magic", "clip.MP4") == "video/mp4"
    assert guess_mime(b"no magic", None) == "application/octet-stream"
    assert extension_for_mime("image/jpeg") == "jpg"
    assert extension_for_mime("audio/wav; codecs=1") == "wav"
    assert extension_for_mime("application/x-unknown-thing") == "bin"
    assert extension_for_mime(None) == "bin"
    assert is_media_filename("a/b/photo.JPEG") and not is_media_filename("notes.txt")


def test_media_item_repr_never_contains_payload():
    item = MediaItem.from_bytes(PNG * 100)
    assert repr(item).startswith("MediaItem<image/png") and "\\x00" not in repr(item)


def test_new_nodes_are_registered_with_schemas():
    reg = build_node_registry()
    assert reg["MediaFileInputNode"] is MediaFileInputNode
    assert reg["MediaFileOutputNode"] is MediaFileOutputNode
    assert get_port_schema("MediaFileInputNode") == {"inputs": [], "outputs": ["out"]}
    assert get_port_schema("MediaFileOutputNode") == {"inputs": ["in"], "outputs": ["out"]}
    assert get_field_schema("MediaFileInputNode")["emit_on"]["options"] == ["change", "start"]
    assert get_field_schema("DirectoryInputNode")["emit_as"]["options"] == ["path", "media"]
    assert "media" in get_field_schema("Base64DecodeNode")["output"]["options"]


# --- uploads into WebInputNode / ApiInputNode ------------------------------------

async def _api_input(config=None):
    uri = _uid("upload")
    node = ApiInputNode(_uid("api-in"), {"uri": uri, "port": 0, **(config or {})})
    await node.init()
    node._running = True
    return node, f"/api/{uri}"


async def test_json_body_still_works_and_non_objects_are_rejected():
    node, path = await _api_input()
    try:
        r = client.post(path, json={"a": 1})
        assert r.status_code == 200 and r.json() == {"status": "ok", "node": node.id}
        assert _emitted(node) == [{"a": 1}]
        assert client.post(path, json=[1, 2]).status_code == 422
        assert client.post(path, content=b"{not json", headers={"Content-Type": "application/json"}).status_code == 422
    finally:
        _nodes.pop(node.id, None)


async def test_multipart_upload_emits_one_media_item_per_file():
    node, path = await _api_input()
    try:
        r = client.post(path, data={"camera": "front"}, files=[
            ("file", ("photo.jpg", JPEG, "image/jpeg")),
            ("file", ("sound.bin", WAV, "application/octet-stream")),
        ])
        assert r.status_code == 200, r.text
        body = r.json()
        assert [m["mime"] for m in body["media"]] == ["image/jpeg", "audio/wav"]
        items = _emitted(node)
        assert all(isinstance(i, MediaItem) for i in items)
        assert items[0].get_bytes() == JPEG
        assert items[0].meta == {"field": "file", "filename": "photo.jpg", "form": {"camera": "front"}}
        assert items[1].kind == "audio"
    finally:
        _nodes.pop(node.id, None)


async def test_multipart_without_files_is_emitted_as_fields():
    node, path = await _api_input()
    try:
        r = client.post(path, data={"name": "x"})
        assert r.status_code == 200
        assert _emitted(node) == [{"name": "x"}]
    finally:
        _nodes.pop(node.id, None)


async def test_raw_body_upload_uses_content_type_or_sniffs(store):
    node, path = await _api_input()
    try:
        r = client.post(path, content=PNG, headers={"Content-Type": "image/png"})
        assert r.status_code == 200 and r.json()["media"][0]["mime"] == "image/png"
        r = client.post(path + "?filename=clip.mp4", content=b"not really an mp4",
                        headers={"Content-Type": "application/octet-stream"})
        assert r.json()["media"][0]["mime"] == "video/mp4"
        r = client.post(path, content=JPEG, headers={"Content-Type": "application/octet-stream", "X-Filename": "../../evil.png"})
        items = _emitted(node)
        assert items[2].mime == "image/jpeg" and items[2].meta["filename"] == "evil.png"
        assert items[0].meta == {}
        assert client.post(path, content=b"", headers={"Content-Type": "image/png"}).status_code == 400
    finally:
        _nodes.pop(node.id, None)


async def test_upload_limit_is_enforced():
    node, path = await _api_input({"max_upload_mb": 0.0001})  # ~104 bytes
    try:
        assert client.post(path, content=b"x" * 500, headers={"Content-Type": "image/png"}).status_code == 413
        assert client.post(path, files={"f": ("a.png", b"x" * 500, "image/png")}).status_code == 413
        assert client.post(path, content=PNG, headers={"Content-Type": "image/png"}).status_code == 200
    finally:
        _nodes.pop(node.id, None)


async def test_web_input_node_accepts_uploads_and_503_when_stopped():
    node = WebInputNode(_uid("web-in"), {"path": f"/in/{_uid('x')}"})
    await node.init()
    try:
        assert client.post(node.path, content=PNG, headers={"Content-Type": "image/png"}).status_code == 503
        node._running = True
        r = client.post(node.path, files={"upload": ("p.png", PNG, "image/png")})
        assert r.status_code == 200 and _emitted(node)[0].mime == "image/png"
    finally:
        _nodes.pop(node.id, None)


# --- serving media from WebOutputNode / ApiOutputNode ----------------------------

async def test_web_output_node_serves_latest_media():
    node = WebOutputNode(_uid("web-out"), {"path": f"/out/{_uid('m')}", "sse": False})
    await node.init()
    try:
        assert node.media_path == node.path + "/media"
        assert client.get(node.media_path).status_code == 404
        node.emit("out", "just text")
        assert client.get(node.path).text == "just text"
        item = MediaItem.from_bytes(WAV * 10, meta={"filename": "t.wav"})
        node.emit("out", item)
        node.emit("out", "text after media")
        r = client.get(node.media_path)
        assert r.status_code == 200 and r.content == WAV * 10
        assert r.headers["content-type"] == "audio/wav"
        assert 't.wav' in r.headers["content-disposition"]
        r = client.get(node.media_path, headers={"Range": "bytes=0-3"})
        assert r.status_code == 206 and r.content == b"RIFF"
        node.emit("out", MediaItem.from_bytes(PNG))
        r = client.get(node.path)
        assert r.headers["content-type"] == "image/png" and r.content == PNG
        assert http_endpoint_info(node)["media_path"] == node.media_path
    finally:
        _nodes.pop(node.id, None)


async def test_api_output_node_media_raw_and_json_routes():
    node = ApiOutputNode(_uid("api-out"), {"uri": _uid("res"), "sse": False, "port": 0})
    await node.init()
    try:
        node.emit("out", MediaItem.from_bytes(JPEG))
        assert client.get(node.media_path).content == JPEG
        assert client.get(node.raw_path).headers["content-type"] == "image/jpeg"
        latest = client.get(node.path).json()["latest"]
        assert latest["$media"] == "image" and latest["mime"] == "image/jpeg"
    finally:
        _nodes.pop(node.id, None)


async def test_expired_media_is_410(store):
    node = ApiOutputNode(_uid("api-out"), {"uri": _uid("gone"), "sse": False, "port": 0})
    await node.init()
    try:
        item = MediaItem.from_bytes(JPEG * 100, inline_limit=10)
        node.emit("out", item)
        store.delete(item.ref)
        assert client.get(node.media_path).status_code == 410
    finally:
        _nodes.pop(node.id, None)


async def test_json_outputs_carry_media_summaries():
    node = WebOutputJSONNode(_uid("json-out"), {"path": f"/oj/{_uid('x')}", "sse": False})
    await node.init()
    try:
        node.emit("out", {"frame": MediaItem.from_bytes(PNG), "raw": b"\x00\x01"})
        latest = client.get(node.path).json()["latest"]
        assert latest["frame"]["mime"] == "image/png" and latest["raw"] == {"$binary": 2, "head": "0001"}
    finally:
        _nodes.pop(node.id, None)


def test_json_payload_helper():
    from pystreamflow.core.media_http import json_payload

    assert json_payload({"a": b"\x01"}) == '{"a": {"$binary": 1, "head": "01"}}'
    assert json_payload({"s": {1, 2}}).startswith('{"s": [')
    assert json_payload(object()).startswith('{"data": "<object')


# --- MediaFileInputNode ----------------------------------------------------------

async def test_media_file_input_emits_whole_file_and_again_on_change(tmp_path):
    f = tmp_path / "pic.png"
    f.write_bytes(PNG)
    node = MediaFileInputNode("mfi", {"path": str(f), "poll_interval": 0.02})
    await node.start()
    try:
        assert await _wait_for(lambda: len(_emitted(node)) == 1)
        item = _emitted(node)[0]
        assert item.mime == "image/png" and item.get_bytes() == PNG
        assert item.meta["filename"] == "pic.png" and item.meta["source_path"] == str(f)
        await asyncio.sleep(0.1)
        assert len(_emitted(node)) == 1  # unchanged file -> no re-emit
        f.write_bytes(JPEG)
        os.utime(f, (time.time() + 5, time.time() + 5))
        assert await _wait_for(lambda: len(_emitted(node)) == 2)
        assert _emitted(node)[1].mime == "image/jpeg"
    finally:
        await node.stop()


async def test_media_file_input_start_mode_missing_file_and_rewire(tmp_path):
    missing = tmp_path / "later.wav"
    node = MediaFileInputNode("mfi2", {"path": str(missing), "emit_on": "start", "poll_interval": 0.02})
    await node.start()
    try:
        assert await _wait_for(lambda: node._last_error and "cannot read" in node._last_error)
        missing.write_bytes(WAV)
        assert await _wait_for(lambda: len(_emitted(node)) == 1)
        missing.write_bytes(WAV + b"more")
        await asyncio.sleep(0.15)
        assert len(_emitted(node)) == 1  # 'start' ignores changes
        other = tmp_path / "other.png"
        other.write_bytes(PNG)
        node.set_attribute("path", str(other))
        assert await _wait_for(lambda: len(_emitted(node)) == 2)
    finally:
        await node.stop()


async def test_media_file_input_rejects_bad_emit_on():
    node = MediaFileInputNode("mfi3", {"path": "x", "emit_on": "sometimes"})
    await node.start()
    assert node.health()["health"] == "error" and "emit_on" in node._last_error


def test_media_file_input_declares_block_media_port():
    from pystreamflow.core.engine import edge_pipe

    pipe = edge_pipe(MediaFileInputNode("x", {}), "out")
    assert pipe.drop_policy == "block" and pipe.queue.maxsize > 0


# --- DirectoryInputNode ------------------------------------------------------------

async def test_directory_input_filters_and_emits_media(tmp_path):
    (tmp_path / "a.png").write_bytes(PNG)
    (tmp_path / "b.JPG").write_bytes(JPEG)
    (tmp_path / "c.txt").write_text("hi")
    (tmp_path / "d.wav").write_bytes(WAV)

    node = DirectoryInputNode("dir1", {"path": str(tmp_path), "extensions": "png, .jpg", "poll_interval": 0.02})
    await node.start()
    try:
        assert await _wait_for(lambda: len(_emitted(node, "files")) == 2)
        assert sorted(os.path.basename(p) for p in _emitted(node, "files")) == ["a.png", "b.JPG"]
    finally:
        await node.stop()

    node = DirectoryInputNode("dir2", {"path": str(tmp_path), "media_only": True, "emit_as": "media", "poll_interval": 0.02})
    await node.start()
    try:
        assert await _wait_for(lambda: len(_emitted(node, "files")) == 3)
        items = _emitted(node, "files")
        assert all(isinstance(i, MediaItem) for i in items)
        assert sorted(i.mime for i in items) == ["audio/wav", "image/jpeg", "image/png"]
    finally:
        await node.stop()


async def test_directory_input_rejects_bad_emit_as(tmp_path):
    node = DirectoryInputNode("dir3", {"path": str(tmp_path), "emit_as": "bytes"})
    await node.start()
    assert node.health()["health"] == "error"


async def test_directory_input_default_behaviour_unchanged(tmp_path):
    (tmp_path / "x.bin").write_bytes(b"1")
    node = DirectoryInputNode("dir4", {"path": str(tmp_path), "poll_interval": 0.02})
    await node.start()
    try:
        assert await _wait_for(lambda: _emitted(node, "files") == [str(tmp_path / "x.bin")])
    finally:
        await node.stop()


# --- MediaFileOutputNode -----------------------------------------------------------

async def test_media_file_output_writes_one_file_per_item(tmp_path):
    pattern = str(tmp_path / "out" / "{node}_{index:03d}_{stem}.{ext}")
    node = MediaFileOutputNode("mfo", {"path_pattern": pattern})
    await node.init()
    r1 = await node.write(MediaItem.from_bytes(PNG, meta={"filename": "cam.png"}))
    r2 = await node.write(JPEG)
    r3 = await node.write("hello")
    assert [os.path.basename(r["path"]) for r in (r1, r2, r3)] == [
        "mfo_000_cam.png", "mfo_001_item.jpg", "mfo_002_item.txt"]
    assert open(r1["path"], "rb").read() == PNG
    assert open(r3["path"]).read() == "hello"
    assert r2 == {"path": r2["path"], "mime": "image/jpeg", "size": len(JPEG)}
    assert _emitted(node) == [r1, r2, r3]
    written = [e for e in node.get_last() if "written" in e]
    assert written[0]["item"].mime == "image/png" and written[0]["bytes_written"] == len(PNG)

    # a new node instance never overwrites existing files
    node2 = MediaFileOutputNode("mfo", {"path_pattern": pattern})
    await node2.init()
    r = await node2.write(MediaItem.from_bytes(PNG, meta={"filename": "cam.png"}))
    assert os.path.basename(r["path"]) == "mfo_001_cam.png"  # 000_cam.png is taken


async def test_media_file_output_without_index_appends_suffix_or_overwrites(tmp_path):
    pattern = str(tmp_path / "{stem}.{ext}")
    node = MediaFileOutputNode("mfo2", {"path_pattern": pattern})
    await node.init()
    paths = [(await node.write(MediaItem.from_bytes(PNG, meta={"filename": "p.png"})))["path"] for _ in range(3)]
    assert [os.path.basename(p) for p in paths] == ["p.png", "p_1.png", "p_2.png"]
    node3 = MediaFileOutputNode("mfo3", {"path_pattern": pattern, "overwrite": True})
    await node3.init()
    r = await node3.write(MediaItem.from_bytes(JPEG, meta={"filename": "p.png"}))
    assert os.path.basename(r["path"]) == "p.jpg"
    r = await node3.write(MediaItem.from_bytes(JPEG, meta={"filename": "p.png"}))
    assert os.path.basename(r["path"]) == "p.jpg"


async def test_media_file_output_default_path_and_errors(tmp_path, monkeypatch, store):
    monkeypatch.setenv("PSF_FILES_DIR", str(tmp_path / "files"))
    node = MediaFileOutputNode("mfo4", {})
    await node.init()
    r = await node.write(MediaItem.from_bytes(WAV))
    assert r["path"] == str(tmp_path / "files" / "media" / "mfo4_00000.wav")

    expired = MediaItem.from_bytes(PNG * 100, inline_limit=10)
    store.delete(expired.ref)
    assert await node.write(expired) is None
    assert "write failed" in node._last_error and node._error_count == 1

    bad = MediaFileOutputNode("mfo5", {"path_pattern": str(tmp_path / "{nope:05d}.{ext}")})
    await bad.init()
    assert await bad.write(PNG) is None


async def test_media_file_output_process_loop(tmp_path):
    node = MediaFileOutputNode("mfo6", {"path_pattern": str(tmp_path / "{index}.{ext}")})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put(MediaItem.from_bytes(PNG))
        assert await _wait_for(lambda: os.path.exists(tmp_path / "0.png"))
    finally:
        await node.stop()


# --- Base64 ------------------------------------------------------------------------

async def _run_once(node_cls, config, item):
    node = node_cls(_uid("b64"), {"auto_start": False, **config})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put(item)
        assert await _wait_for(lambda: len(_emitted(node)) == 1)
        return _emitted(node)[0]
    finally:
        await node.stop()


async def test_base64_encode_media_and_data_url():
    item = MediaItem.from_bytes(PNG * 100, inline_limit=10)
    assert await _run_once(Base64EncodeNode, {}, item) == base64.b64encode(PNG * 100).decode()
    url = await _run_once(Base64EncodeNode, {"data_url": True}, item)
    assert url == "data:image/png;base64," + base64.b64encode(PNG * 100).decode()
    assert (await _run_once(Base64EncodeNode, {"data_url": True}, JPEG)).startswith("data:image/jpeg;base64,")
    assert (await _run_once(Base64EncodeNode, {"data_url": True}, "hi")) == "data:text/plain;base64,aGk="
    assert (await _run_once(Base64EncodeNode, {}, "hi")) == "aGk="


async def test_base64_decode_data_url_and_output_modes():
    b64 = base64.b64encode(PNG).decode()
    item = await _run_once(Base64DecodeNode, {}, f"data:image/png;base64,{b64}")
    assert isinstance(item, MediaItem) and item.mime == "image/png" and item.get_bytes() == PNG
    assert await _run_once(Base64DecodeNode, {}, "data:text/plain;charset=utf-8;base64,aGk=") == "hi"
    assert await _run_once(Base64DecodeNode, {}, "aGk=") == "hi"  # unchanged default
    assert await _run_once(Base64DecodeNode, {}, b64) == PNG  # plain base64 -> bytes as before
    assert await _run_once(Base64DecodeNode, {"output": "bytes"}, "aGk=") == b"hi"
    forced = await _run_once(Base64DecodeNode, {"output": "media"}, b64)
    assert isinstance(forced, MediaItem) and forced.mime == "image/png"
    assert await _run_once(Base64DecodeNode, {"output": "text"}, f"data:image/png;base64,{b64}") == PNG


async def test_base64_decode_rejects_bad_output_mode():
    node = Base64DecodeNode("b64bad", {"output": "pdf"})
    await node.start()
    assert node.health()["health"] == "error"
