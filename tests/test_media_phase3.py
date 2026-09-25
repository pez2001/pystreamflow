"""Media plan phase 3 (docs/plans/media_types_plan.md): Pillow image nodes,
their optional registration, and vision input for LMStudioNode."""
import asyncio
import base64
import io
import json
import subprocess
import sys
import time

import httpx
import pytest

PIL = pytest.importorskip("PIL")
from PIL import Image  # noqa: E402

from pystreamflow.core.blob_store import BlobStore, set_blob_store  # noqa: E402
from pystreamflow.core.media import MediaItem  # noqa: E402
from pystreamflow.core.stream import Pipe  # noqa: E402
from pystreamflow.nodes import image_nodes as im  # noqa: E402
from pystreamflow.nodes.llm_lmstudio import LMStudioNode  # noqa: E402


@pytest.fixture(autouse=True)
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"))
    set_blob_store(s)
    yield s
    set_blob_store(None)


def make_image(size=(40, 20), color=(200, 30, 30), mode="RGB", fmt="PNG", exif=None, **meta) -> MediaItem:
    img = Image.new(mode, size, color)
    if mode == "RGB":
        # left half red, right half blue - makes flips/crops checkable
        img.paste((30, 30, 200), (size[0] // 2, 0, size[0], size[1]))
    buf = io.BytesIO()
    kwargs = {"exif": exif} if exif is not None else {}
    img.save(buf, format=fmt, **kwargs)
    return MediaItem.from_bytes(buf.getvalue(), meta=meta)


def decode(item: MediaItem) -> Image.Image:
    img = Image.open(io.BytesIO(item.get_bytes()))
    img.load()
    return img


async def run(cls, config, item):
    node = cls("n", config)
    await node.init()
    return node, await node.handle(item)


# --- helpers ----------------------------------------------------------------------

def test_parse_format_and_encode():
    assert im.parse_format("keep") is None and im.parse_format("") is None
    assert im.parse_format("JPG") == "JPEG"
    with pytest.raises(ValueError):
        im.parse_format("psd")
    rgba = Image.new("RGBA", (4, 4), (0, 0, 0, 0))
    jpeg = Image.open(io.BytesIO(im.encode_image(rgba, "JPEG")))
    assert jpeg.mode == "RGB" and jpeg.getpixel((0, 0)) == (255, 255, 255)  # flattened onto white
    for fmt in ("GIF", "BMP", "TIFF", "WEBP"):
        assert Image.open(io.BytesIO(im.encode_image(rgba, fmt))).format == fmt
    cmyk = Image.new("CMYK", (2, 2))
    assert Image.open(io.BytesIO(im.encode_image(cmyk, "PNG"))).mode == "RGB"


# --- individual nodes ---------------------------------------------------------------

async def test_decode_fills_meta_and_corrects_mime():
    item = make_image(fmt="JPEG", filename="a.png")
    wrong = MediaItem(kind="image", mime="image/png", data=item.data, meta=item.meta)
    node, out = await run(im.ImageDecodeNode, {}, wrong)
    assert out.mime == "image/jpeg"
    assert out.meta == {"filename": "a.png", "width": 40, "height": 20, "mode": "RGB", "format": "JPEG"}
    assert out.data == item.data  # payload untouched


async def test_decode_auto_orient_uses_exif():
    exif = Image.Exif()
    exif[0x0112] = 6  # rotated 90° clockwise
    item = make_image(size=(40, 20), fmt="JPEG", exif=exif)
    _, plain = await run(im.ImageDecodeNode, {}, item)
    assert (plain.meta["width"], plain.meta["height"]) == (40, 20)
    _, upright = await run(im.ImageDecodeNode, {"auto_orient": True}, item)
    assert (upright.meta["width"], upright.meta["height"]) == (20, 40)
    assert decode(upright).size == (20, 40)
    _, info = await run(im.ImageInfoNode, {}, item)
    assert info["exif"]["Orientation"] == 6


@pytest.mark.parametrize("config,expected", [
    ({"max_side": 10}, (10, 5)),
    ({"width": 20}, (20, 10)),
    ({"height": 5}, (10, 5)),
    ({"width": 10, "height": 10}, (10, 5)),
    ({"width": 10, "height": 10, "keep_aspect": False}, (10, 10)),
    ({"max_side": 80}, (40, 20)),  # no upscale by default
    ({"max_side": 80, "upscale": True, "resample": "nearest"}, (80, 40)),
])
async def test_resize(config, expected):
    item = make_image(meta_key="x")
    _, out = await run(im.ImageResizeNode, config, item)
    assert decode(out).size == expected
    assert (out.meta.get("width", 40), out.meta.get("height", 20)) == expected
    if expected == (40, 20):
        assert out is item  # unchanged size -> passed on as-is


async def test_resize_requires_a_size():
    node = im.ImageResizeNode("n", {})
    with pytest.raises(ValueError):
        await node.init()
    with pytest.raises(ValueError):
        await im.ImageResizeNode("n", {"max_side": 10, "resample": "cubic"}).init()


async def test_crop_box_center_and_clamp():
    item = make_image()
    _, out = await run(im.ImageCropNode, {"width": 10, "height": 10, "x": 25, "y": 0}, item)
    img = decode(out)
    assert img.size == (10, 10) and img.getpixel((0, 0))[2] > 150  # right half = blue
    _, out = await run(im.ImageCropNode, {"width": 10, "height": 10, "center": True}, item)
    assert decode(out).size == (10, 10)
    _, out = await run(im.ImageCropNode, {"width": 100, "height": 100, "x": 5}, item)
    assert decode(out).size == (40, 20)
    with pytest.raises(ValueError):
        await im.ImageCropNode("n", {"width": 10}).init()


async def test_rotate():
    item = make_image()
    _, out = await run(im.ImageRotateNode, {"angle": 90}, item)
    assert decode(out).size == (20, 40)
    _, out = await run(im.ImageRotateNode, {"angle": -90}, item)
    assert decode(out).size == (20, 40)
    _, out = await run(im.ImageRotateNode, {"angle": 45}, item)
    w, h = decode(out).size
    assert w > 40 and h > 20
    _, out = await run(im.ImageRotateNode, {"angle": 360}, item)
    assert out is item
    _, out = await run(im.ImageRotateNode, {"angle": "exif"}, item)
    assert decode(out).size == (40, 20)
    p_item = MediaItem.from_bytes(im.encode_image(Image.new("P", (6, 4)), "PNG"))
    _, out = await run(im.ImageRotateNode, {"angle": 30}, p_item)
    assert out is not p_item and out.mime == "image/png"


async def test_flip():
    item = make_image()
    _, out = await run(im.ImageFlipNode, {}, item)
    assert decode(out).getpixel((0, 0))[2] > 150  # blue now on the left
    _, out = await run(im.ImageFlipNode, {"direction": "both"}, item)
    assert decode(out).getpixel((0, 0))[2] > 150
    with pytest.raises(ValueError):
        await im.ImageFlipNode("n", {"direction": "diagonal"}).init()


async def test_convert():
    item = make_image(mode="RGBA", color=(0, 0, 0, 0), filename="t.png")
    _, out = await run(im.ImageConvertNode, {"format": "jpeg", "quality": 70}, item)
    assert out.mime == "image/jpeg" and decode(out).format == "JPEG"
    assert out.meta["filename"] == "t.png"
    _, out = await run(im.ImageConvertNode, {"format": "png", "mode": "L"}, item)
    assert out.mime == "image/png" and decode(out).mode == "L"
    _, out = await run(im.ImageConvertNode, {"format": "webp"}, item)
    assert out.mime == "image/webp"
    _, out = await run(im.ImageConvertNode, {"format": "keep", "mode": "rgba"}, make_image())
    assert out.mime == "image/png" and decode(out).mode == "RGBA"
    for bad in ({"quality": 0}, {"quality": 101}, {"mode": "CMYK"}, {"format": "svg"}):
        with pytest.raises(ValueError):
            await im.ImageConvertNode("n", bad).init()


async def test_filter():
    item = make_image(size=(20, 20))
    _, blurred = await run(im.ImageFilterNode, {"filter": "gaussian_blur", "radius": 3}, item)
    assert decode(blurred).getpixel((10, 10)) != decode(item).getpixel((10, 10))
    _, dark = await run(im.ImageFilterNode, {"brightness": 0.5}, item)
    assert decode(dark).getpixel((0, 0))[0] < 120
    for f in ("blur", "box_blur", "unsharp_mask", "edges", "emboss", "none"):
        _, out = await run(im.ImageFilterNode, {"filter": f, "contrast": 1.2, "saturation": 0.8, "sharpness": 1.5}, item)
        assert decode(out).size == (20, 20)
    p_item = MediaItem.from_bytes(im.encode_image(Image.new("P", (6, 4)), "PNG"))
    _, out = await run(im.ImageFilterNode, {"filter": "sharpen"}, p_item)
    assert decode(out).mode == "RGB"
    for bad in ({"filter": "oil"}, {"brightness": -1}):
        with pytest.raises(ValueError):
            await im.ImageFilterNode("n", bad).init()


async def test_info_is_a_json_safe_dict():
    item = make_image(mode="RGBA", color=(1, 2, 3, 4), filename="x.png", form={"a": "b"})
    _, info = await run(im.ImageInfoNode, {"exif": False}, item)
    assert info == {
        "mime": "image/png", "kind": "image", "size": item.size(), "width": 40, "height": 20,
        "mode": "RGBA", "format": "PNG", "frames": 1, "has_alpha": True, "meta": {"filename": "x.png"},
    }
    json.dumps(info)


def test_exif_value_conversion():
    assert im._json_safe(b"Canon\x00") == "Canon"
    assert im._json_safe(b"\xff\x00") == "ff00"
    assert im._json_safe((1, [2, b"x"])) == [1, [2, "x"]]
    assert im._json_safe({1: None}) == {"1": None}
    from PIL.TiffImagePlugin import IFDRational
    assert im._json_safe(IFDRational(1, 2)) == 0.5
    assert im._json_safe(object()).startswith("<object")


async def test_thumbnail():
    item = make_image(size=(400, 100))
    _, out = await run(im.ImageThumbnailNode, {}, item)
    assert out.mime == "image/webp" and decode(out).size == (256, 64) and out.meta["thumbnail"] is True
    _, out = await run(im.ImageThumbnailNode, {"max_side": 50, "format": "jpeg"}, item)
    assert out.mime == "image/jpeg" and decode(out).size == (50, 13)
    with pytest.raises(ValueError):
        await im.ImageThumbnailNode("n", {"format": "gif"}).init()


# --- shared MediaTransformNode behaviour ------------------------------------------------

async def test_non_images_pass_through_untouched():
    node = im.ImageResizeNode("n", {"max_side": 10})
    await node.init()
    audio = MediaItem.from_bytes(b"RIFF\x00\x00\x00\x00WAVE" + b"\x00" * 20)
    assert await node.handle("text") == "text"
    assert await node.handle(audio) is audio
    assert node._error_count == 0


async def test_corrupt_image_passes_through_and_records_error():
    node = im.ImageResizeNode("n", {"max_side": 10})
    await node.init()
    broken = MediaItem(kind="image", mime="image/png", data=b"\x89PNG\r\n\x1a\nnot really")
    assert await node.handle(broken) is broken
    assert node._error_count == 1 and node._last_error


async def test_video_frames_keep_kind_and_meta_and_large_items_use_blob_store(store):
    frame = make_image(size=(200, 100))
    frame = MediaItem(kind="video_frame", mime=frame.mime, data=frame.data, meta={"pts": 7})
    _, out = await run(im.ImageResizeNode, {"max_side": 50}, frame)
    assert out.kind == "video_frame" and out.meta["pts"] == 7

    big = MediaItem.from_bytes(make_image(size=(300, 300)).data, inline_limit=10)
    assert big.ref and not big.is_inline
    _, out = await run(im.ImageFlipNode, {}, big)
    assert decode(out).size == (300, 300)


async def test_process_loop_and_invalid_config_marks_node_error():
    node = im.ImageThumbnailNode("thumb", {"max_side": 8})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put(make_image())
        deadline = time.monotonic() + 3
        while not node.get_last() and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        assert decode(node.get_last()[-1]["item"]).size == (8, 4)
    finally:
        await node.stop()
    bad = im.ImageResizeNode("bad", {})
    await bad.start()
    assert bad.health()["health"] == "error"


# --- registration -----------------------------------------------------------------------

def test_image_nodes_registered_with_schemas():
    from pystreamflow.core.config_schema import get_field_schema
    from pystreamflow.core.registry import build_node_registry
    from pystreamflow.nodes import UNAVAILABLE_NODE_TYPES

    reg = build_node_registry()
    for cls in im.IMAGE_NODE_CLASSES:
        assert reg[cls.__name__] is cls
    assert UNAVAILABLE_NODE_TYPES == {}
    assert "lanczos" in get_field_schema("ImageResizeNode")["resample"]["options"]


def test_without_pillow_image_nodes_are_reported_unavailable():
    code = (
        "import sys, json; sys.modules['PIL'] = None\n"
        "from pystreamflow.core.registry import build_node_registry\n"
        "from pystreamflow.nodes import UNAVAILABLE_NODE_TYPES\n"
        "print(json.dumps({'registered': [k for k in build_node_registry() if k.startswith('Image')],"
        " 'unavailable': UNAVAILABLE_NODE_TYPES}))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    result = json.loads(out.stdout.strip().splitlines()[-1])
    assert result["registered"] == []
    assert set(result["unavailable"]) == {c.__name__ for c in im.IMAGE_NODE_CLASSES}
    assert "pystreamflow[image]" in result["unavailable"]["ImageResizeNode"]


def test_node_availability_endpoint(monkeypatch):
    from fastapi.testclient import TestClient

    from conftest import TEST_API_KEY
    from pystreamflow import nodes
    from pystreamflow.api.server import app

    monkeypatch.setitem(nodes.UNAVAILABLE_NODE_TYPES, "ImageResizeNode", "needs Pillow")
    r = TestClient(app, headers={"Authorization": f"Bearer {TEST_API_KEY}"}).get("/node-availability")
    assert r.json() == {"unavailable": {"ImageResizeNode": "needs Pillow"}}


# --- LMStudioNode vision -------------------------------------------------------------------

def _patch_client(monkeypatch, handler):
    import pystreamflow.nodes.llm_lmstudio as llm_module

    original = llm_module.httpx.AsyncClient

    def patched(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(handler)
        return original(*args, **kwargs)

    monkeypatch.setattr(llm_module.httpx, "AsyncClient", patched)


async def test_lmstudio_sends_images_as_image_url_parts(monkeypatch, store):
    sent = []

    def handler(request):
        sent.append(json.loads(request.content))
        return httpx.Response(200, json={"choices": [{"message": {"content": "a red square"}}]})

    _patch_client(monkeypatch, handler)
    node = LMStudioNode("vision", {"image_prompt": "What is this?"})
    inp, prompt_pipe = Pipe(), Pipe()
    node.add_input("in", inp)
    node.add_input("prompt", prompt_pipe)
    await node.start()
    try:
        image = make_image(fmt="JPEG")

        async def next_result():
            deadline = time.monotonic() + 3
            n = len(sent)
            while len(sent) == n and time.monotonic() < deadline:
                await asyncio.sleep(0.02)
            return sent[-1]["messages"][-1]["content"]

        await inp.put(image)
        content = await next_result()
        assert content[0] == {"type": "text", "text": "What is this?"}
        assert content[1]["image_url"]["url"] == "data:image/jpeg;base64," + base64.b64encode(image.data).decode()

        await prompt_pipe.put({"value": "Count the colors."})
        await asyncio.sleep(0.3)
        await inp.put(image)
        assert (await next_result())[0]["text"] == "Count the colors."

        big = MediaItem.from_bytes(make_image(size=(60, 60)).data, inline_limit=10)
        await inp.put({"image": big, "prompt": "Explicit question", "other": make_image()})
        content = await next_result()
        assert content[0]["text"] == "Explicit question" and len(content) == 3

        await inp.put([image, image])
        assert len(await next_result()) == 3

        await inp.put("plain text")
        assert await next_result() == "plain text"
        assert _emitted_results(node) == ["a red square"] * 5
    finally:
        await node.stop()


def _emitted_results(node):
    return [e["item"] for e in node.get_last() if e.get("port") == "results"]


async def test_lmstudio_reports_expired_image(monkeypatch, store):
    _patch_client(monkeypatch, lambda request: httpx.Response(500))
    node = LMStudioNode("vision2", {})
    inp = Pipe()
    node.add_input("in", inp)
    await node.start()
    try:
        big = MediaItem.from_bytes(make_image(size=(60, 60)).data, inline_limit=10)
        store.delete(big.ref)
        await inp.put(big)
        deadline = time.monotonic() + 3
        while not [e for e in node.get_last() if e.get("port") == "errors"] and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        errors = [e["item"] for e in node.get_last() if e.get("port") == "errors"]
        assert errors == ["image payload expired before it could be sent"]
    finally:
        await node.stop()


def test_lmstudio_port_schema_has_prompt_input():
    from pystreamflow.core.port_schema import get_port_schema

    assert get_port_schema("LMStudioNode")["inputs"] == ["in", "prompt"]


def test_engine_rejects_unavailable_node_types(monkeypatch):
    from pystreamflow import nodes
    from pystreamflow.core.engine import Engine
    from pystreamflow.core.models import Graph, Node

    monkeypatch.setitem(nodes.UNAVAILABLE_NODE_TYPES, "ImageResizeNode", "needs Pillow")
    g = Graph()
    g.add_node(Node(id="r", type="ImageResizeNode", config={"max_side": 10}))
    with pytest.raises(ValueError, match="needs Pillow"):
        Engine(g).validate()
