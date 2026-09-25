"""Media plan phase 5 (docs/plans/media_types_plan.md): video nodes on PyAV
with the ffmpeg subprocess fallback."""
import asyncio
import fractions
import io
import json
import math
import os
import subprocess
import sys
import time

import pytest

av = pytest.importorskip("av")
np = pytest.importorskip("numpy")

from pystreamflow.core import ffmpeg  # noqa: E402
from pystreamflow.core.blob_store import BlobStore, set_blob_store  # noqa: E402
from pystreamflow.core.media import MediaItem  # noqa: E402
from pystreamflow.core.stream import Pipe  # noqa: E402
from pystreamflow.nodes import video_nodes as vn  # noqa: E402

needs_ffmpeg = pytest.mark.skipif(ffmpeg.ffmpeg_path() is None or vn.ffprobe_path() is None,
                                  reason="ffmpeg/ffprobe not installed")
BACKENDS = ["pyav", pytest.param("ffmpeg", marks=needs_ffmpeg)]


@pytest.fixture(autouse=True)
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"))
    set_blob_store(s)
    yield s
    set_blob_store(None)


def make_video(seconds=2.0, fps=10, size=(160, 120), audio=True, fmt="mp4") -> bytes:
    """A small test clip: the frame colour steps each second, 440 Hz tone."""
    buf = io.BytesIO()
    with av.open(buf, "w", format=fmt) as c:
        v = c.add_stream("libx264" if fmt == "mp4" else "libvpx-vp9", rate=fps)
        v.width, v.height = size
        v.pix_fmt = "yuv420p"
        a = None
        if audio:
            a = c.add_stream("aac" if fmt == "mp4" else "libopus", rate=48000)
            a.layout = "mono"
        for i in range(int(seconds * fps)):
            shade = 40 + 150 * (int(i / fps) % 2)
            img = np.full((size[1], size[0], 3), (shade, 60, 200 - shade), dtype=np.uint8)
            frame = av.VideoFrame.from_ndarray(img, format="rgb24")
            frame.pts = i
            for p in v.encode(frame):
                c.mux(p)
        for p in v.encode(None):
            c.mux(p)
        if a is not None:
            n = 1024 if fmt == "mp4" else 960
            total = int(seconds * 48000)
            for start in range(0, total, n):
                t = (np.arange(start, min(start + n, total)) / 48000).astype(np.float32)
                samples = (0.3 * np.sin(2 * math.pi * 440 * t)).astype(np.float32)[None, :]
                af = av.AudioFrame.from_ndarray(samples, format="fltp", layout="mono")
                af.sample_rate, af.pts = 48000, start
                for p in a.encode(af):
                    c.mux(p)
            for p in a.encode(None):
                c.mux(p)
    return buf.getvalue()


@pytest.fixture(scope="module")
def clip() -> bytes:
    return make_video()


def video_item(data, **meta):
    return MediaItem.from_bytes(data, meta={"filename": "clip.mp4", **meta})


def emitted(node, port="out"):
    return [e["item"] for e in node.get_last() if e.get("port") == port]


async def wait_for(cond, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return cond()


async def decoder(config):
    node = vn.VideoDecodeNode("dec", {"max_last": 1000, **config})
    await node.init()
    node._running = True
    return node


# --- helpers ------------------------------------------------------------------------------

def test_small_helpers(monkeypatch):
    assert vn._quality_to_q(100) == 2 and vn._quality_to_q(1) == 31
    assert vn._fit(1920, 1080, 640) == (640, 360) and vn._fit(100, 50, 0) == (100, 50)
    assert vn._fit(101, 51, 1000) == (101, 51)
    assert vn._rate("30000/1001") == pytest.approx(29.97, abs=0.01) and vn._rate("bad") == 0.0
    assert vn._jpeg_size(b"not a jpeg") == (None, None)
    assert vn.pick_backend({}) == "pyav"
    with pytest.raises(ValueError):
        vn.pick_backend({"backend": "gstreamer"})
    monkeypatch.setattr(vn, "HAVE_PYAV", False)
    monkeypatch.setenv("PSF_FFMPEG", "/nonexistent/ffmpeg")
    with pytest.raises(ValueError, match="pystreamflow\\[video\\]"):
        vn.pick_backend({"backend": "pyav"})
    with pytest.raises(ValueError, match="or the ffmpeg executable"):
        vn.pick_backend({})
    assert not vn.available()


# --- VideoDecodeNode ------------------------------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
async def test_decode_all_frames(clip, backend):
    node = await decoder({"backend": backend})
    assert await node.decode(video_item(clip)) == 20
    frames = emitted(node)
    assert [f.meta["index"] for f in frames] == list(range(20))
    assert [f.meta["pts"] for f in frames[:3]] == [0.0, 0.1, 0.2]
    assert frames[0].kind == "video_frame" and frames[0].mime == "image/jpeg"
    assert (frames[0].meta["width"], frames[0].meta["height"]) == (160, 120)
    assert frames[0].meta["fps"] == 10 and frames[0].meta["filename"] == "clip.mp4"
    assert [f.meta["last"] for f in frames].count(True) == 1 and frames[-1].meta["last"]
    assert frames[0].get_bytes()[:3] == b"\xff\xd8\xff"


@pytest.mark.parametrize("backend", BACKENDS)
async def test_decode_sampled_and_scaled(clip, backend):
    node = await decoder({"backend": backend, "fps": 2, "max_side": 80, "quality": 50})
    assert await node.decode(video_item(clip)) == 4
    frames = emitted(node)
    assert [f.meta["pts"] for f in frames] == [0.0, 0.5, 1.0, 1.5]
    assert (frames[0].meta["width"], frames[0].meta["height"]) == (80, 60)
    assert frames[0].meta["fps"] == 2


async def test_decode_png_keyframes_and_audio(clip):
    node = await decoder({"frame_format": "png", "audio": True, "audio_chunk_ms": 250, "audio_sample_rate": 16000})
    await node.decode(video_item(clip))
    frames, audio = emitted(node), emitted(node, "audio")
    assert frames[0].mime == "image/png" and frames[0].meta["keyframe"] is True
    assert audio and all(a.kind == "audio_chunk" and a.mime == "audio/wav" for a in audio)
    assert audio[0].meta["sample_rate"] == 16000 and audio[0].meta["channels"] == 1
    assert audio[0].meta["frames"] == 4000 and audio[1].meta["pts"] == pytest.approx(audio[0].meta["pts"] + 0.25, abs=1e-3)
    total = sum(a.meta["duration"] for a in audio)
    assert total == pytest.approx(2.0, abs=0.1)


async def test_decode_blob_stored_input_and_errors(clip):
    big = MediaItem.from_bytes(clip, inline_limit=10)
    assert big.ref and not big.is_inline
    node = await decoder({"fps": 1})
    assert await node.decode(big) == 2
    with pytest.raises(Exception):
        await node.decode(MediaItem(kind="video", mime="video/mp4", data=b"\x00\x00\x00\x18ftypisom broken"))
    assert not await node._decode_safely(MediaItem(kind="video", mime="video/mp4", data=b"garbage"))
    assert node._error_count == 1 and node._last_error


async def test_decode_rejects_bad_config():
    for bad in ({"frame_format": "gif"}, {"quality": 0}, {"quality": 101}, {"fps": -1}):
        with pytest.raises(ValueError):
            await vn.VideoDecodeNode("d", bad).init()


@needs_ffmpeg
async def test_ffmpeg_backend_limits():
    for bad in ({"backend": "ffmpeg", "frame_format": "png"}, {"backend": "ffmpeg", "audio": True}):
        with pytest.raises(ValueError, match="ffmpeg backend"):
            await vn.VideoDecodeNode("d", bad).init()
    with pytest.raises(ffmpeg.FFmpegError):
        list(vn.iter_ffmpeg("/nonexistent/clip.mp4", False, 0, 0, 85))


async def test_decode_process_source_path_and_in_port(clip, tmp_path):
    path = tmp_path / "clip.mp4"
    path.write_bytes(clip)
    node = vn.VideoDecodeNode("dec-src", {"source": str(path), "fps": 5, "max_last": 1000})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        assert await wait_for(lambda: len(emitted(node)) == 10)
        assert emitted(node)[0].meta["source"] == str(path)
        await pipe.put("not a video")
        await pipe.put(video_item(clip))
        assert await wait_for(lambda: len(emitted(node)) == 21)
        assert emitted(node)[10] == "not a video"
    finally:
        await node.stop()


async def test_stream_url_reconnects_after_failure():
    node = vn.VideoDecodeNode("cam", {"source": "rtsp://127.0.0.1:9/nothing", "reconnect_s": 0.05})
    await node.start()
    try:
        assert await wait_for(lambda: node._error_count >= 2, timeout=10)
    finally:
        await node.stop()


def test_media_source_meta_and_cleanup(clip):
    src = vn._MediaSource(video_item(clip, form={"a": "b"}, width=1))
    path = src.path
    assert os.path.exists(path) and src.meta == {"filename": "clip.mp4", "form": {"a": "b"}}
    src.close()
    assert not os.path.exists(path)
    assert vn._MediaSource("rtsp://cam/1").is_stream and not vn._MediaSource("/tmp/x.mp4").is_stream


# --- frame nodes through image nodes ----------------------------------------------------------

async def test_frames_work_with_image_nodes(clip):
    pytest.importorskip("PIL")
    from pystreamflow.nodes.image_nodes import ImageResizeNode

    node = await decoder({"fps": 1})
    await node.decode(video_item(clip))
    resize = ImageResizeNode("r", {"max_side": 40})
    await resize.init()
    out = await resize.handle(emitted(node)[0])
    assert out.kind == "video_frame" and out.meta["width"] == 40 and out.meta["pts"] == 0.0


# --- VideoFrameSampleNode ---------------------------------------------------------------------

def frame(index, pts, keyframe=False, fps=10):
    return MediaItem(kind="video_frame", mime="image/jpeg", data=b"\xff\xd8\xff",
                     meta={"index": index, "pts": pts, "keyframe": keyframe, "fps": fps})


async def sampler(config):
    node = vn.VideoFrameSampleNode("s", config)
    await node.init()
    return node


async def test_frame_sample_modes():
    frames = [frame(i, i / 10, keyframe=(i % 8 == 0)) for i in range(25)]
    every = await sampler({"n": 10})
    assert [f.meta["index"] for f in frames if every.keep(f)] == [0, 10, 20]
    fps = await sampler({"mode": "fps", "fps": 4})
    assert [f.meta["index"] for f in frames if fps.keep(f)] == [0, 3, 5, 8, 10, 13, 15, 18, 20, 23]
    key = await sampler({"mode": "keyframes"})
    assert [f.meta["index"] for f in frames if key.keep(f)] == [0, 8, 16, 24]
    # a new stream (index going back) restarts the count
    assert every.keep(frame(0, 0.0)) and not every.keep(frame(1, 0.1))
    no_pts = await sampler({"mode": "fps", "fps": 5})
    kept = [i for i in range(10) if no_pts.keep(MediaItem(kind="video_frame", mime="image/jpeg", data=b"x",
                                                            meta={"fps": 10}))]
    assert kept == [0, 2, 4, 6, 8]
    for bad in ({"mode": "random"}, {"n": 0}, {"mode": "fps", "fps": 0}):
        with pytest.raises(ValueError):
            await vn.VideoFrameSampleNode("s", bad).init()


async def test_frame_sample_process_loop():
    node = vn.VideoFrameSampleNode("s2", {"n": 2})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        for i in range(4):
            await pipe.put(frame(i, i / 10))
        await pipe.put("text")
        assert await wait_for(lambda: len(emitted(node)) == 3)
        assert [getattr(i, "meta", {}).get("index", i) for i in emitted(node)] == [0, 2, "text"]
    finally:
        await node.stop()


# --- VideoInfoNode / VideoThumbnailNode -----------------------------------------------------

@pytest.mark.parametrize("backend", BACKENDS)
async def test_video_info(clip, backend):
    node = vn.VideoInfoNode("i", {"backend": backend})
    await node.init()
    info = await node.handle(video_item(clip))
    assert info["filename"] == "clip.mp4" and info["mime"] == "video/mp4"
    assert (info["video_codec"], info["width"], info["height"], info["fps"]) == ("h264", 160, 120, 10.0)
    assert info["duration"] == pytest.approx(2.0, abs=0.1) and info["frames"] == 20
    assert (info["audio_codec"], info["sample_rate"], info["channels"]) == ("aac", 48000, 1)
    json.dumps(info)
    assert await node.handle("text") == "text"
    assert await node.handle(MediaItem(kind="video", mime="video/mp4", data=b"junk")) is None
    assert node._error_count == 1


@pytest.mark.parametrize("backend", BACKENDS)
async def test_video_thumbnail(clip, backend):
    node = vn.VideoThumbnailNode("t", {"backend": backend, "at_s": 1.5, "max_side": 80})
    await node.init()
    thumb = await node.handle(video_item(clip))
    assert thumb.kind == "image" and thumb.mime == "image/jpeg" and thumb.meta["filename"] == "clip.jpg"
    assert thumb.meta["width"] == 80 and thumb.meta["source_pts"] == pytest.approx(1.5, abs=0.11)
    img = av.open(io.BytesIO(thumb.get_bytes()))
    rgb = next(img.decode(video=0)).to_ndarray(format="rgb24")
    assert rgb[..., 0].mean() > 120  # second second is the bright frame colour
    late = vn.VideoThumbnailNode("t2", {"backend": backend, "at_percent": 100})
    await late.init()
    assert (await late.handle(video_item(clip))).meta["source_pts"] <= 2.0
    with pytest.raises(ValueError):
        await vn.VideoThumbnailNode("t3", {"at_percent": 150}).init()
    assert await node.handle(MediaItem(kind="video", mime="video/mp4", data=b"junk")) is None


# --- VideoEncodeNode ----------------------------------------------------------------------

async def frames_and_audio(clip, **config):
    node = await decoder({"audio": True, **config})
    await node.decode(video_item(clip))
    return emitted(node), emitted(node, "audio")


def probe(item):
    with av.open(io.BytesIO(item.get_bytes())) as c:
        return {s.type: (s.codec_context.name, s.frames) for s in c.streams}, float(c.duration or 0) / 1e6


async def encoder(config, audio_port=False):
    node = vn.VideoEncodeNode("enc", {"flush_idle_s": 0, **config})
    await node.init()
    if audio_port:
        node.inputs["audio"] = Pipe()
    return node


@pytest.mark.parametrize("backend", BACKENDS)
async def test_encode_frames_with_audio(clip, backend):
    frames, audio = await frames_and_audio(clip, fps=5)
    node = await encoder({"backend": backend}, audio_port=True)
    for a in audio:
        await node.add_audio(a)    # arrives before the frames -> backlog
    for f in frames:
        await node.add_frame(f)
    out = await node.finish()      # 'last' frame armed the audio grace period
    streams, duration = probe(out)
    assert out.kind == "video" and out.mime == "video/mp4" and out.meta["filename"] == "clip.mp4"
    assert streams["video"] == ("h264", 10) and streams["audio"][0] == "aac"
    assert duration == pytest.approx(2.0, abs=0.25)
    assert (out.meta["frames"], out.meta["width"], out.meta["fps"]) == (10, 160, 5.0)


async def test_encode_segments_webm_images_and_resizing(clip):
    frames, _ = await frames_and_audio(clip, fps=5)
    node = await encoder({"segment_s": 1.0, "format": "webm", "crf": 40})
    for f in frames:
        await node.add_frame(f)
    outs = emitted(node)
    assert [o.meta["frames"] for o in outs] == [5, 5] and outs[0].mime == "video/webm"
    assert [o.meta["pts"] for o in outs] == [0.0, 1.0]
    assert probe(outs[0])[0]["video"][0] == "vp9"

    # plain image items without pts/fps meta, of changing size
    node = await encoder({"fps": 4})
    for i, size in enumerate([(64, 48), (32, 24), (64, 48)]):
        data = vn._FrameEncoder("jpeg", 90).encode(av.VideoFrame.from_ndarray(
            np.zeros((size[1], size[0], 3), dtype=np.uint8), format="rgb24"))
        await node.add_frame(MediaItem.from_bytes(data))
    out = await node.finish()
    assert (out.meta["width"], out.meta["height"], out.meta["frames"]) == (64, 48, 3)
    assert out.meta["duration"] == pytest.approx(0.75, abs=0.01)
    assert await node.finish() is None
    for bad in ({"format": "avi"},):
        with pytest.raises(ValueError):
            await vn.VideoEncodeNode("e", bad).init()


async def test_encode_process_ports_flush_and_stop(clip):
    frames, audio = await frames_and_audio(clip, fps=5)
    node = vn.VideoEncodeNode("enc-loop", {"flush_idle_s": 0, "audio_grace_s": 0.2})
    inp, aud, flush = Pipe(), Pipe(), Pipe()
    node.add_input("in", inp)
    node.add_input("audio", aud)
    node.add_input("flush", flush)
    await node.start()
    try:
        for f in frames:
            await inp.put(f)
        for a in audio:
            await aud.put(a)   # lags behind the last frame, still included
        assert await wait_for(lambda: len(emitted(node)) == 1)
        streams, _ = probe(emitted(node)[0])
        assert "audio" in streams
        for f in frames[:3]:
            f.meta["last"] = False
            await inp.put(f)
        await asyncio.sleep(0.3)
        await flush.put("now")
        assert await wait_for(lambda: len(emitted(node)) == 2)
        await inp.put("text")
        assert await wait_for(lambda: emitted(node)[-1] == "text")
        await inp.put(frames[0])
        await asyncio.sleep(0.3)
    finally:
        await node.stop()
    assert len([o for o in emitted(node) if isinstance(o, MediaItem)]) == 3  # finished on stop


async def test_encode_idle_flush_and_non_wav_audio(clip):
    pytest.importorskip("soundfile")
    from pystreamflow.nodes.audio_nodes import encode_audio

    frames, _ = await frames_and_audio(clip, fps=5)
    node = vn.VideoEncodeNode("enc-idle", {"flush_idle_s": 0.3})
    inp = Pipe()
    node.add_input("in", inp)
    node.inputs["audio"] = Pipe()
    await node.start()
    try:
        flac, mime = encode_audio(np.zeros((4800, 1), dtype=np.float32), 48000, "flac")
        if ffmpeg.ffmpeg_path():
            await node.add_audio(MediaItem.from_bytes(flac, mime=mime))
        for f in frames[:4]:
            f.meta["last"] = False
            await inp.put(f)
        assert await wait_for(lambda: len(emitted(node)) == 1)
    finally:
        await node.stop()


# --- registration -----------------------------------------------------------------------

def test_video_nodes_registered_with_schemas():
    from pystreamflow.core.config_schema import get_field_schema
    from pystreamflow.core.port_schema import get_port_schema
    from pystreamflow.core.registry import build_node_registry

    reg = build_node_registry()
    for cls in vn.VIDEO_NODE_CLASSES:
        assert reg[cls.__name__] is cls
    assert get_port_schema("VideoDecodeNode")["outputs"] == ["out", "audio"]
    assert get_port_schema("VideoEncodeNode")["inputs"] == ["in", "audio", "flush"]
    assert get_field_schema("VideoFrameSampleNode")["mode"]["options"] == ["every_n", "fps", "keyframes"]


@pytest.mark.parametrize("ffmpeg_env,expected", [("/nonexistent/ffmpeg", "unavailable"), (None, "registered")])
def test_registration_without_pyav(ffmpeg_env, expected):
    if expected == "registered" and ffmpeg.ffmpeg_path() is None:
        pytest.skip("ffmpeg not installed")
    env = {**os.environ}
    if ffmpeg_env:
        env["PSF_FFMPEG"] = ffmpeg_env
    code = (
        "import sys, json; sys.modules['av'] = None\n"
        "from pystreamflow.core.registry import build_node_registry\n"
        "from pystreamflow.nodes import UNAVAILABLE_NODE_TYPES\n"
        "print(json.dumps({'registered': sorted(k for k in build_node_registry() if k.startswith('Video')),"
        " 'unavailable': sorted(k for k in UNAVAILABLE_NODE_TYPES if k.startswith('Video'))}))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True, env=env)
    result = json.loads(out.stdout.strip().splitlines()[-1])
    names = sorted(c.__name__ for c in vn.VIDEO_NODE_CLASSES)
    assert result[expected] == names
    assert result["registered" if expected == "unavailable" else "unavailable"] == []


def test_frame_encoder_reuses_context_and_handles_size_change():
    enc = vn._FrameEncoder("png", 85)
    small = av.VideoFrame.from_ndarray(np.zeros((8, 8, 3), dtype=np.uint8), format="rgb24")
    big = av.VideoFrame.from_ndarray(np.zeros((16, 16, 3), dtype=np.uint8), format="rgb24")
    assert enc.encode(small)[:4] == b"\x89PNG" and enc.encode(big)[:4] == b"\x89PNG"
    assert fractions.Fraction(1, 25) == enc._ctx.time_base or enc._ctx is None


async def test_decode_fps_sampling_stays_on_grid():
    # 25 fps source sampled at 2 fps: frames closest after 0, 0.5, 1.0, 1.5 -
    # not drifting 0, 0.52, 1.04, 1.56
    node = await decoder({"fps": 2})
    await node.decode(video_item(make_video(seconds=2.0, fps=25, audio=False)))
    assert [f.meta["pts"] for f in emitted(node)] == [0.0, 0.52, 1.0, 1.52]
