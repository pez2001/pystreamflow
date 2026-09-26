"""Media plan phase 7 (docs/plans/media_types_plan.md): media over MCP,
Docker targets, example workflows."""
import asyncio
import base64
import io
import json
import logging
import os
import pathlib
import time
import uuid

import pytest

from pystreamflow.core.blob_store import BlobStore, set_blob_store
from pystreamflow.core.media import MediaItem
from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes
from pystreamflow.mcp import media_tools
from pystreamflow.mcp.server import _execute_tool, _unwrap, mcp_server

REPO = pathlib.Path(__file__).resolve().parent.parent
PNG_1PX = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
)


@pytest.fixture(autouse=True)
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"))
    set_blob_store(s)
    yield s
    set_blob_store(None)


@pytest.fixture
def media_dirs(tmp_path, monkeypatch):
    files = tmp_path / "files"
    data = tmp_path / "data"
    files.mkdir()
    data.mkdir()
    monkeypatch.setenv("PSF_FILES_DIR", str(files))
    monkeypatch.setenv("PSF_DATA_DIR", str(data))
    monkeypatch.delenv("PSF_MCP_MEDIA_ROOTS", raising=False)
    return files


def run(coro):
    return asyncio.run(coro)


# --- $media payloads ------------------------------------------------------------------

def test_media_from_path_is_limited_to_allowed_roots(media_dirs, tmp_path, monkeypatch):
    (media_dirs / "dot.png").write_bytes(PNG_1PX)
    item = media_tools.media_from_spec({"path": str(media_dirs / "dot.png")})
    assert item.mime == "image/png" and item.meta["filename"] == "dot.png"
    outside = tmp_path / "secret.png"
    outside.write_bytes(PNG_1PX)
    with pytest.raises(ValueError, match="outside the allowed media directories"):
        media_tools.media_from_spec({"path": str(outside)})
    with pytest.raises(ValueError, match="outside"):
        media_tools.media_from_spec({"path": str(media_dirs / ".." / "secret.png")})
    monkeypatch.setenv("PSF_MCP_MEDIA_ROOTS", str(tmp_path))
    assert media_tools.media_from_spec({"path": str(outside)}).get_bytes() == PNG_1PX
    with pytest.raises(ValueError, match="no such file"):
        media_tools.media_from_spec({"path": str(media_dirs / "missing.png")})
    monkeypatch.setenv("PSF_MAX_UPLOAD_MB", "0.00001")
    with pytest.raises(ValueError, match="larger than"):
        media_tools.media_from_spec({"path": str(media_dirs / "dot.png")})


def test_media_from_base64_and_ref(store, media_dirs):
    item = media_tools.media_from_spec({"base64": base64.b64encode(PNG_1PX).decode(), "filename": "a.png"})
    assert item.mime == "image/png" and item.meta == {"filename": "a.png"}
    url = "data:audio/wav;base64," + base64.b64encode(b"RIFF\x00\x00\x00\x00WAVEfmt ").decode()
    assert media_tools.media_from_spec({"base64": url}).mime == "audio/wav"
    with pytest.raises(ValueError, match="invalid base64"):
        media_tools.media_from_spec({"base64": "a"})
    ref = store.put(PNG_1PX)
    by_ref = media_tools.media_from_spec({"ref": ref})
    assert by_ref.ref == ref and by_ref.kind == "image" and by_ref.get_bytes() == PNG_1PX
    assert media_tools.media_from_spec({"ref": f"/media/{ref}?mime=image/png"}).ref == ref
    with pytest.raises(ValueError, match="unknown or expired"):
        media_tools.media_from_spec({"ref": "0" * 64})
    for bad in ({}, "x"):
        with pytest.raises(ValueError):
            media_tools.media_from_spec(bad)


def test_resolve_media_payload(media_dirs):
    spec = {"$media": {"base64": base64.b64encode(PNG_1PX).decode()}}
    assert isinstance(media_tools.resolve_media_payload(spec), MediaItem)
    nested = media_tools.resolve_media_payload({"image": spec, "prompt": "hi"})
    assert isinstance(nested["image"], MediaItem) and nested["prompt"] == "hi"
    plain = {"msg": "hello", "$media_like": 1}
    assert media_tools.resolve_media_payload(plain) == plain
    assert media_tools.resolve_media_payload("text") == "text"


async def test_send_to_node_injects_media(media_dirs):
    from pystreamflow.nodes.display import DisplayNode

    node_id = f"mcp-media-{uuid.uuid4().hex[:6]}"
    node = DisplayNode(node_id, {"auto_start": False})
    pipe = Pipe()
    node.add_input("in", pipe)
    _nodes[node_id] = node
    try:
        (media_dirs / "dot.png").write_bytes(PNG_1PX)
        result = await _execute_tool("send_to_node", {"node_id": node_id, "payload": {"$media": {"path": "dot.png"}}})
        assert "outside the allowed" in result["error"]  # relative to cwd, not the files dir
        result = await _execute_tool("send_to_node", {"node_id": node_id,
                                                      "payload": {"$media": {"path": str(media_dirs / "dot.png")}}})
        assert result == {"result": {"status": "sent", "node": node_id}}
        item = await asyncio.wait_for(pipe.get(), 2)
        assert isinstance(item, MediaItem) and item.meta["filename"] == "dot.png"
    finally:
        _nodes.pop(node_id, None)


# --- get_media -----------------------------------------------------------------------

@pytest.fixture
def media_node():
    from pystreamflow.nodes.display import DisplayNode

    node_id = f"mcp-get-{uuid.uuid4().hex[:6]}"
    node = DisplayNode(node_id, {})
    _nodes[node_id] = node
    yield node
    _nodes.pop(node_id, None)


def test_get_media_via_call_returns_base64(media_node):
    assert "no media item" in run(_execute_tool("get_media", {"node_id": media_node.id}))["error"]
    media_node.emit("out", MediaItem.from_bytes(PNG_1PX, meta={"filename": "p.png"}))
    result = _unwrap(run(_execute_tool("get_media", {"node_id": media_node.id})))
    assert result["mime"] == "image/png" and base64.b64decode(result["base64"]) == PNG_1PX
    assert result["summary"]["meta"]["filename"] == "p.png"
    by_ref = _unwrap(run(_execute_tool("get_media", {"ref": result["summary"]["ref"]})))
    assert by_ref["base64"] == result["base64"]
    for args, err in (({}, "pass ref"), ({"node_id": "nope"}, "node not found")):
        assert err in run(_execute_tool("get_media", args))["error"]


def test_get_media_mcp_content_types(media_node):
    PIL = pytest.importorskip("PIL")
    from PIL import Image

    buf = io.BytesIO()
    Image.new("RGB", (2000, 1000), (1, 2, 3)).save(buf, format="PNG")
    media_node.emit("out", MediaItem.from_bytes(buf.getvalue()))
    result = run(mcp_server.call_tool("get_media", {"node_id": media_node.id, "max_side": 200}))
    image, text = result.content
    assert image.type == "image" and image.mime_type == "image/jpeg"
    assert Image.open(io.BytesIO(base64.b64decode(image.data))).size == (200, 100)
    assert json.loads(text.text)["returned_as"] == {"mime": "image/jpeg", "max_side": 200}

    full = run(mcp_server.call_tool("get_media", {"node_id": media_node.id, "max_side": 0}))
    assert Image.open(io.BytesIO(base64.b64decode(full.content[0].data))).size == (2000, 1000)

    media_node.emit("out", MediaItem.from_bytes(b"RIFF\x00\x00\x00\x00WAVEfmt " + b"\x00" * 32))
    audio = run(mcp_server.call_tool("get_media", {"node_id": media_node.id}))
    assert audio.content[0].type == "audio" and audio.content[0].mime_type == "audio/wav"

    media_node.emit("out", MediaItem.from_bytes(b"\x00\x00\x00\x18ftypisom" + b"\x00" * 32))
    video = run(mcp_server.call_tool("get_media", {"node_id": media_node.id}))
    assert [c.type for c in video.content] == ["text"] and json.loads(video.content[0].text)["$media"] == "video"


def test_get_media_size_cap(media_node, monkeypatch):
    media_node.emit("out", MediaItem.from_bytes(PNG_1PX))
    monkeypatch.setenv("PSF_MCP_MEDIA_MAX_MB", "0.00001")
    assert "PSF_MCP_MEDIA_MAX_MB" in run(_execute_tool("get_media", {"node_id": media_node.id, "max_side": 0}))["error"]


def test_get_media_tool_is_listed():
    from fastapi.testclient import TestClient
    from pystreamflow.mcp.server import app

    names = [t["name"] for t in TestClient(app).get("/tools").json()["tools"]]
    assert "get_media" in names


# --- JSONOutputNode --------------------------------------------------------------------

async def test_json_output_serializes_media():
    from pystreamflow.nodes.json_output import JSONOutputNode

    node = JSONOutputNode("j", {"pretty": False, "auto_start": False})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put({"frame": MediaItem.from_bytes(PNG_1PX, meta={"pts": 1.5}), "answer": "ok"})
        deadline = time.monotonic() + 3
        while not node.get_last() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        data = json.loads(node.get_last()[-1]["item"])
        assert data["answer"] == "ok" and data["frame"]["meta"]["pts"] == 1.5 and data["frame"]["$media"] == "image"
        assert node._last_error is None
    finally:
        await node.stop()


# --- example workflows -----------------------------------------------------------------

EXAMPLES = ["image_thumbnails", "audio_transcribe", "video_vision"]


@pytest.mark.parametrize("name", EXAMPLES)
def test_example_workflows_validate_without_type_warnings(name, caplog):
    from pystreamflow.core.engine import Engine
    from pystreamflow.core.persistence import load_workflow
    from pystreamflow.nodes import UNAVAILABLE_NODE_TYPES

    graph = load_workflow(str(REPO / "workflows" / f"{name}.yaml"))
    types = {n.type for n in graph.nodes}
    if types & set(UNAVAILABLE_NODE_TYPES):
        pytest.skip("media extras not installed")
    with caplog.at_level(logging.WARNING, logger="pystreamflow.engine"):
        assert Engine(graph).validate() is True
    assert not [r for r in caplog.records if "workflow edge" in r.getMessage()]


async def test_image_thumbnails_example_end_to_end(tmp_path, monkeypatch):
    pytest.importorskip("PIL")
    from PIL import Image

    from pystreamflow.core.engine import Engine
    from pystreamflow.core.persistence import load_workflow

    monkeypatch.chdir(tmp_path)
    photos = tmp_path / "files" / "photos"
    photos.mkdir(parents=True)
    Image.new("RGB", (1600, 1200), (10, 20, 30)).save(photos / "wide.png")
    exif = Image.Exif()
    exif[0x0112] = 6
    Image.new("RGB", (1200, 900), (200, 20, 30)).save(photos / "phone.jpg", exif=exif)
    (photos / "notes.txt").write_text("skip me")

    graph = load_workflow(str(REPO / "workflows" / "image_thumbnails.yaml"))
    for node in graph.nodes:
        if node.id == "photos":
            node.config["poll_interval"] = 0.05
    engine = Engine(graph)
    task = asyncio.create_task(engine.run())
    out = tmp_path / "files" / "thumbnails"
    try:
        deadline = time.monotonic() + 10
        while len(list(out.glob("*.webp"))) < 2 and time.monotonic() < deadline:
            await asyncio.sleep(0.05)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        for node in graph.nodes:
            _nodes.pop(node.id, None)
    sizes = {p.name: Image.open(p).size for p in out.glob("*.webp")}
    assert sizes == {"wide.webp": (800, 600), "phone.webp": (600, 800)}


# --- Docker ----------------------------------------------------------------------------

def test_dockerfile_targets_and_compose_selection():
    dockerfile = (REPO / "Dockerfile").read_text()
    stages = [line.split(" AS ")[1].strip() for line in dockerfile.splitlines() if line.startswith("FROM ") and " AS " in line]
    assert stages[-1] == "runtime" and "runtime-media" in stages  # plain `docker build .` stays slim
    assert "apt-get install -y --no-install-recommends ffmpeg" in dockerfile
    run_lines = [line for line in dockerfile.splitlines() if not line.lstrip().startswith("#")]
    assert not any("pip install fastapi" in line for line in run_lines)  # no hand-maintained dependency list
    for compose in ("docker-compose.yml", "docker-compose.prod.yml"):
        text = (REPO / compose).read_text()
        assert "target: ${PSF_IMAGE_TARGET:-runtime}" in text and "PSF_MEDIA_EXTRAS" in text
    assert "${PSF_MEMORY_LIMIT:-512M}" in (REPO / "docker-compose.prod.yml").read_text()
