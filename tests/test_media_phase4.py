"""Media plan phase 4 (docs/plans/media_types_plan.md): audio nodes on
numpy + soundfile (ffmpeg fallback) and SpeechToTextNode."""
import asyncio
import io
import json
import subprocess
import sys
import time
import types

import httpx
import pytest

np = pytest.importorskip("numpy")
sf = pytest.importorskip("soundfile")

from pystreamflow.core import ffmpeg  # noqa: E402
from pystreamflow.core.blob_store import BlobStore, set_blob_store  # noqa: E402
from pystreamflow.core.media import MediaItem  # noqa: E402
from pystreamflow.core.stream import Pipe  # noqa: E402
from pystreamflow.nodes import audio_nodes as an  # noqa: E402
from pystreamflow.nodes.speech_to_text import SpeechToTextNode  # noqa: E402

needs_ffmpeg = pytest.mark.skipif(ffmpeg.ffmpeg_path() is None, reason="ffmpeg not installed")
SR = 16000


@pytest.fixture(autouse=True)
def store(tmp_path):
    s = BlobStore(str(tmp_path / "blobs"))
    set_blob_store(s)
    yield s
    set_blob_store(None)


def tone(seconds, freq=440.0, sr=SR, amp=0.5, channels=1):
    t = np.arange(int(round(seconds * sr))) / sr
    x = (amp * np.sin(2 * np.pi * freq * t)).astype(np.float32)
    return np.repeat(x[:, None], channels, axis=1)


def silence(seconds, sr=SR, channels=1):
    return np.zeros((int(round(seconds * sr)), channels), dtype=np.float32)


def wav_item(samples, sr=SR, kind="audio", **meta):
    return an.audio_item(samples, sr, kind, "wav", meta)


def dominant_freq(samples, sr):
    spectrum = np.abs(np.fft.rfft(samples[:, 0]))
    return np.fft.rfftfreq(samples.shape[0], 1 / sr)[int(np.argmax(spectrum))]


def emitted(node, port="out"):
    return [e["item"] for e in node.get_last() if e.get("port") == port]


async def wait_for(cond, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if cond():
            return True
        await asyncio.sleep(0.02)
    return cond()


# --- helpers --------------------------------------------------------------------------

@pytest.mark.parametrize("fmt,mime", [("wav", "audio/wav"), ("flac", "audio/flac"), ("ogg", "audio/ogg"), ("mp3", "audio/mpeg")])
def test_encode_decode_roundtrip(fmt, mime):
    data, got_mime = an.encode_audio(tone(0.5, sr=44100), 44100, fmt)
    assert got_mime == mime
    samples, sr = an.decode_audio(MediaItem.from_bytes(data, mime=mime))
    assert sr == 44100 and abs(samples.shape[0] - 22050) < 2500
    assert abs(dominant_freq(samples, sr) - 440) < 10


def test_encode_clips_and_rejects_unknown_format():
    data, _ = an.encode_audio(tone(0.1, amp=3.0), SR, "wav")
    samples, _ = sf.read(io.BytesIO(data), always_2d=True)
    assert samples.max() <= 1.0
    with pytest.raises(ValueError):
        an.encode_audio(tone(0.1), SR, "wma")


@needs_ffmpeg
def test_ffmpeg_formats_and_fallback_decode():
    m4a, mime = an.encode_audio(tone(0.5, sr=44100), 44100, "m4a")
    assert mime == "audio/mp4"
    samples, sr = an.decode_audio(MediaItem.from_bytes(m4a, mime="audio/mp4"))  # not readable by libsndfile
    assert sr == 44100 and abs(dominant_freq(samples, sr) - 440) < 15
    opus, mime = an.encode_audio(tone(0.2, sr=48000), 48000, "opus")
    assert mime == "audio/ogg" and opus[:4] == b"OggS"
    # MP3 at a rate libsndfile's encoder refuses -> ffmpeg takes over
    mp3, mime = an.encode_audio(tone(0.2, sr=7000), 7000, "mp3")
    assert mime == "audio/mpeg" and len(mp3) > 100


def test_undecodable_and_missing_ffmpeg(monkeypatch):
    with pytest.raises(ValueError, match="cannot decode"):
        an.decode_audio(MediaItem.from_bytes(b"definitely not audio", mime="audio/mp4"))
    monkeypatch.setenv("PSF_FFMPEG", "/nonexistent/ffmpeg")
    assert ffmpeg.ffmpeg_path() is None
    with pytest.raises(ffmpeg.FFmpegNotFound):
        ffmpeg.convert(b"x", "wav")
    with pytest.raises(ValueError, match="ffmpeg not found"):
        an.decode_audio(MediaItem.from_bytes(b"xx", mime="audio/aac"))


@needs_ffmpeg
def test_ffmpeg_error_is_reported():
    with pytest.raises(ffmpeg.FFmpegError, match="ffmpeg failed"):
        ffmpeg.convert(b"garbage", "wav", input_ext="mp4")


def test_resample_and_remix():
    x = tone(1.0, freq=1000, sr=48000, channels=2)
    y = an.resample(x, 48000, 16000)
    assert y.shape == (16000, 2) and abs(dominant_freq(y, 16000) - 1000) < 5
    up = an.resample(tone(0.5, sr=8000), 8000, 16000)
    assert up.shape[0] == 8000
    assert an.resample(x, 48000, 48000) is x
    assert an.remix(x, 1).shape[1] == 1
    assert an.remix(tone(0.1), 2).shape[1] == 2
    assert an.remix(tone(0.1, channels=4), 2).shape[1] == 2
    assert an.remix(tone(0.1, channels=2), 3).shape[1] == 3
    assert an.remix(x, 2) is x


def test_levels_and_db():
    rms, peak = an.levels(tone(1.0, amp=0.5))
    assert abs(peak - 0.5) < 1e-3 and abs(rms - 0.5 / np.sqrt(2)) < 1e-3
    assert an.to_db(1.0) == 0 and an.to_db(0) == an.DB_FLOOR
    assert an.levels(np.zeros((0, 1))) == (0.0, 0.0)
    db = an.window_db(np.concatenate([silence(0.1), tone(0.1)]), 320)
    assert db[0] == an.DB_FLOOR and db[-1] > -10
    assert an.window_db(silence(0.001), 320).size == 0


# --- per-item nodes -----------------------------------------------------------------

async def run(cls, config, item):
    node = cls("n", config)
    await node.init()
    return node, await node.handle(item)


async def test_resample_node_keeps_kind_and_meta():
    item = wav_item(tone(0.5, sr=44100, channels=2), 44100, kind="audio_chunk", pts=1.5, filename="a.wav")
    _, out = await run(an.AudioResampleNode, {"sample_rate": 16000, "channels": 1}, item)
    assert out.kind == "audio_chunk" and out.mime == "audio/wav"
    assert out.meta["pts"] == 1.5 and out.meta["filename"] == "a.wav"
    assert (out.meta["sample_rate"], out.meta["channels"], out.meta["frames"]) == (16000, 1, 8000)
    _, same = await run(an.AudioResampleNode, {"sample_rate": "", "channels": "keep"}, item)
    assert same.meta["sample_rate"] == 44100
    for bad in ({"sample_rate": 10}, {"channels": 0}):
        with pytest.raises(ValueError):
            await an.AudioResampleNode("n", bad).init()


async def test_gain_and_normalize():
    item = wav_item(tone(0.5, amp=0.25))
    _, louder = await run(an.AudioGainNode, {"gain_db": 6.0206}, item)
    assert abs(an.levels(an.decode_audio(louder)[0])[1] - 0.5) < 0.01
    _, peak = await run(an.AudioNormalizeNode, {}, item)
    assert abs(an.to_db(an.levels(an.decode_audio(peak)[0])[1]) - (-1)) < 0.2
    _, rms = await run(an.AudioNormalizeNode, {"mode": "rms", "target_db": -20}, item)
    assert abs(an.to_db(an.levels(an.decode_audio(rms)[0])[0]) - (-20)) < 0.2
    quiet = wav_item(tone(0.5, amp=0.0005))
    _, limited = await run(an.AudioNormalizeNode, {"max_gain_db": 6}, quiet)
    assert an.levels(an.decode_audio(limited)[0])[1] < 0.002
    _, silent = await run(an.AudioNormalizeNode, {}, wav_item(silence(0.2)))
    assert an.levels(an.decode_audio(silent)[0])[1] == 0
    with pytest.raises(ValueError):
        await an.AudioNormalizeNode("n", {"mode": "lufs"}).init()


async def test_level_node_emits_dict_and_numbers():
    node, info = await run(an.AudioLevelNode, {}, wav_item(tone(0.5, amp=0.5), kind="audio_chunk", pts=2.0))
    assert info["peak_db"] == pytest.approx(-6.02, abs=0.05) and info["silent"] is False and info["pts"] == 2.0
    assert emitted(node, "rms_db") == [info["rms_db"]] and emitted(node, "peak_db") == [info["peak_db"]]
    _, quiet = await run(an.AudioLevelNode, {"silence_db": -30}, wav_item(silence(0.2)))
    assert quiet["silent"] is True and quiet["rms_db"] == an.DB_FLOOR
    json.dumps(info)


async def test_non_audio_passes_through():
    node, out = await run(an.AudioGainNode, {"gain_db": 3}, "text")
    assert out == "text" and node._error_count == 0


# --- AudioDecodeNode ------------------------------------------------------------------

async def test_decode_whole_and_chunked():
    flac = MediaItem.from_bytes(an.encode_audio(tone(1.05, channels=2), SR, "flac")[0], meta={"filename": "x.flac"})
    node = an.AudioDecodeNode("dec", {})
    await node.init()
    assert await node.handle(flac) == 1
    whole = emitted(node)[0]
    assert whole.kind == "audio" and whole.mime == "audio/wav" and whole.meta["channels"] == 2
    assert whole.meta["filename"] == "x.flac" and whole.meta["pts"] == 0.0

    node = an.AudioDecodeNode("dec2", {"chunk_ms": 250})
    await node.init()
    assert await node.handle(flac) == 5
    chunks = emitted(node)
    assert [c.meta["index"] for c in chunks] == [0, 1, 2, 3, 4]
    assert [c.meta["pts"] for c in chunks] == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert [c.meta["last"] for c in chunks] == [False] * 4 + [True]
    assert all(c.kind == "audio_chunk" for c in chunks) and chunks[-1].meta["frames"] == 800


@needs_ffmpeg
def _mp4_with_audio(tmp_path):
    wav = tmp_path / "t.wav"
    sf.write(str(wav), tone(0.5), SR)
    out = tmp_path / "v.mp4"
    subprocess.run([ffmpeg.ffmpeg_path(), "-loglevel", "error", "-f", "lavfi", "-i", "color=c=red:s=64x64:d=0.5",
                    "-i", str(wav), "-shortest", "-c:v", "mpeg4", "-c:a", "aac", "-y", str(out)], check=True)
    return out.read_bytes()


@needs_ffmpeg
async def test_decode_audio_track_of_a_video(tmp_path):
    video = MediaItem.from_bytes(_mp4_with_audio(tmp_path))
    assert video.kind == "video"
    node = an.AudioDecodeNode("dec3", {})
    await node.init()
    assert await node.handle(video) == 1
    out = emitted(node)[0]
    assert out.kind == "audio" and abs(dominant_freq(an.decode_audio(out)[0], out.meta["sample_rate"]) - 440) < 15


async def test_decode_errors_and_passthrough():
    node = an.AudioDecodeNode("dec4", {"chunk_ms": 100})
    await node.init()
    assert await node.handle("text") == 1 and emitted(node) == ["text"]
    broken = MediaItem(kind="audio", mime="audio/wav", data=b"RIFF\x00\x00\x00\x00WAVEjunk")
    assert await node.handle(broken) == 0 and node._error_count == 1
    with pytest.raises(ValueError):
        await an.AudioDecodeNode("dec5", {"chunk_ms": -1}).init()


async def test_decode_process_loop_uses_block_policy():
    from pystreamflow.core.engine import edge_pipe

    assert edge_pipe(an.AudioDecodeNode("d", {}), "out").drop_policy == "block"
    node = an.AudioDecodeNode("dec6", {"chunk_ms": 500})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put(wav_item(tone(1.0)))
        assert await wait_for(lambda: len(emitted(node)) == 2)
    finally:
        await node.stop()


# --- AudioEncodeNode ------------------------------------------------------------------

async def chunks_of(samples, chunk_s=0.25, sr=SR, **meta):
    node = an.AudioDecodeNode("chunker", {"chunk_ms": chunk_s * 1000})
    await node.init()
    await node.handle(wav_item(samples, sr, **meta))
    return emitted(node)


async def test_encode_collects_chunks_until_last():
    node = an.AudioEncodeNode("enc", {"format": "flac", "flush_idle_s": 0})
    await node.init()
    for c in await chunks_of(tone(1.0), filename="rec.wav"):
        await node.accept(c)
    out = emitted(node)
    assert len(out) == 1 and out[0].mime == "audio/flac" and out[0].kind == "audio"
    assert out[0].meta["frames"] == SR and out[0].meta["pts"] == 0.0 and out[0].meta["filename"] == "rec.wav"
    assert "index" not in out[0].meta and "last" not in out[0].meta


async def test_encode_segments_and_sample_rate_change():
    node = an.AudioEncodeNode("enc2", {"format": "wav", "segment_s": 0.4, "flush_idle_s": 0})
    await node.init()
    for c in await chunks_of(tone(1.0)):
        await node.accept(c)
    out = emitted(node)
    assert [o.meta["frames"] for o in out] == [6400, 6400, 3200]
    assert [o.meta["pts"] for o in out] == [0.0, 0.4, 0.8]

    node = an.AudioEncodeNode("enc3", {"flush_idle_s": 0})
    await node.init()
    await node.accept(wav_item(tone(0.2), kind="audio_chunk"))
    await node.accept(wav_item(tone(0.2, sr=8000), 8000, kind="audio_chunk"))  # forces a flush
    assert [o.meta["sample_rate"] for o in emitted(node)] == [SR]
    await node.accept("not audio")
    assert emitted(node)[-1] == "not audio"
    with pytest.raises(ValueError):
        await an.AudioEncodeNode("bad", {"format": "wma"}).init()


async def test_encode_flush_port_and_idle_flush():
    node = an.AudioEncodeNode("enc4", {"format": "mp3", "flush_idle_s": 0})
    inp, flush = Pipe(), Pipe()
    node.add_input("in", inp)
    node.add_input("flush", flush)
    await node.start()
    try:
        await inp.put(wav_item(tone(0.3), kind="audio_chunk"))
        await asyncio.sleep(0.3)
        assert emitted(node) == []
        await flush.put("now")
        assert await wait_for(lambda: len(emitted(node)) == 1)
        assert emitted(node)[0].mime == "audio/mpeg"
    finally:
        await node.stop()

    node = an.AudioEncodeNode("enc5", {"flush_idle_s": 0.3})
    inp = Pipe()
    node.add_input("in", inp)
    await node.start()
    try:
        await inp.put(wav_item(tone(0.3), kind="audio_chunk"))
        assert await wait_for(lambda: len(emitted(node)) == 1, timeout=2)
    finally:
        await node.stop()


# --- AudioSegmentNode --------------------------------------------------------------------

def speech_like():
    # 0.5 s silence, 1.0 s tone, 1.0 s silence, 0.8 s tone, 0.3 s silence
    return np.concatenate([silence(0.5), tone(1.0), silence(1.0), tone(0.8, freq=660), silence(0.3)])


async def test_segment_at_silence_whole_clip():
    node = an.AudioSegmentNode("seg", {"flush_idle_s": 0})
    await node.init()
    await node.accept(wav_item(speech_like(), filename="talk.wav"))
    segs = emitted(node)
    assert len(segs) == 2
    assert segs[0].meta["pts"] == pytest.approx(0.3, abs=0.03)
    assert segs[0].meta["duration"] == pytest.approx(1.4, abs=0.05)  # 0.2 pad + 1.0 + 0.2 pad
    assert segs[1].meta["pts"] == pytest.approx(2.3, abs=0.03)
    assert segs[1].meta["duration"] == pytest.approx(1.2, abs=0.05)  # 0.2 pad + 0.8 + 0.2 pad
    assert [s.meta["index"] for s in segs] == [0, 1] and segs[0].meta["filename"] == "talk.wav"
    assert abs(dominant_freq(an.decode_audio(segs[1])[0], SR) - 660) < 10


async def test_segment_streaming_chunks_matches_whole():
    node = an.AudioSegmentNode("seg2", {"flush_idle_s": 0})
    await node.init()
    for c in await chunks_of(speech_like(), chunk_s=0.1):
        await node.accept(c)
    segs = emitted(node)
    assert [round(s.meta["pts"], 1) for s in segs] == [0.3, 2.3]


async def test_segment_limits_and_time_mode():
    # a 50 ms blip is dropped, a long tone is cut at max_segment_s
    node = an.AudioSegmentNode("seg3", {"flush_idle_s": 0, "max_segment_s": 1.0, "pad_ms": 0})
    await node.init()
    await node.accept(wav_item(np.concatenate([tone(0.05), silence(1.0), tone(2.5)])))
    # cuts land on the 20 ms analysis windows
    assert [s.meta["duration"] for s in emitted(node)] == pytest.approx([1.0, 1.0, 0.5], abs=0.021)

    node = an.AudioSegmentNode("seg4", {"mode": "time", "segment_s": 0.4, "flush_idle_s": 0})
    await node.init()
    await node.accept(wav_item(silence(1.0)))
    assert [round(s.meta["duration"], 2) for s in emitted(node)] == [0.4, 0.4, 0.2]

    node = an.AudioSegmentNode("seg5", {"flush_idle_s": 0})
    await node.init()
    await node.accept(wav_item(silence(2.0)))
    assert emitted(node) == []
    await node.accept(wav_item(np.concatenate([silence(0.3), tone(0.5)]), kind="audio"))
    assert len(emitted(node)) == 1  # voiced tail flushed with the whole clip
    for bad in ({"mode": "vad"}, {"segment_s": 0}):
        with pytest.raises(ValueError):
            await an.AudioSegmentNode("bad", bad).init()


# --- SpeechToTextNode -------------------------------------------------------------------

async def stt(config, handler, item):
    node = SpeechToTextNode("stt", config)
    await node.init()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        result = await node.handle(item, client)
    return node, result


async def test_stt_api_backend_sends_multipart_and_emits_text():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers.get("authorization")
        seen["body"] = request.content
        return httpx.Response(200, json={
            "text": " Hallo Welt ", "language": "de", "duration": 1.0,
            "segments": [{"start": 0.0, "end": 1.0, "text": " Hallo Welt", "tokens": [1]}],
        })

    item = wav_item(tone(1.0), kind="audio_chunk", pts=4.0, index=2, filename="rec.wav")
    node, details = await stt({"base_url": "http://stt:8000/v1/", "api_key": "sk", "language": "de",
                               "prompt": "PyStreamFlow"}, handler, item)
    assert seen["url"] == "http://stt:8000/v1/audio/transcriptions" and seen["auth"] == "Bearer sk"
    body = seen["body"]
    assert b'name="model"\r\n\r\nwhisper-1' in body and b'name="language"\r\n\r\nde' in body
    assert b'name="prompt"\r\n\r\nPyStreamFlow' in body and b'filename="rec.wav"' in body
    assert emitted(node) == ["Hallo Welt"]
    assert details["segments"] == [{"start": 0.0, "end": 1.0, "text": "Hallo Welt"}]
    assert details["pts"] == 4.0 and details["source"] == {"filename": "rec.wav", "index": 2}
    assert emitted(node, "details")[0]["language"] == "de"


async def test_stt_api_errors_and_plain_text_response():
    node, result = await stt({}, lambda r: httpx.Response(500, text="model not loaded"), wav_item(tone(0.2)))
    assert result is None and emitted(node, "errors") == ["RuntimeError: HTTP 500: model not loaded"]
    def named(request):
        assert b'filename="talk.wav"' in request.content  # WAV data, not .m4a
        return httpx.Response(200, text="plain transcript")

    node, result = await stt({}, named, wav_item(tone(0.2), filename="talk.m4a"))
    assert result["text"] == "plain transcript"
    node, result = await stt({}, lambda r: httpx.Response(200), "not audio")
    assert result is None and "expected an audio item" in emitted(node, "errors")[0]


async def test_stt_process_loop_uses_its_own_client(monkeypatch):
    import pystreamflow.nodes.speech_to_text as mod

    original = mod.httpx.AsyncClient
    monkeypatch.setattr(mod.httpx, "AsyncClient", lambda *a, **k: original(
        *a, transport=httpx.MockTransport(lambda r: httpx.Response(200, json={"text": "ok"})), **k))
    node = SpeechToTextNode("stt-loop", {})
    pipe = Pipe()
    node.add_input("in", pipe)
    await node.start()
    try:
        await pipe.put(wav_item(tone(0.2)))
        assert await wait_for(lambda: emitted(node) == ["ok"])
    finally:
        await node.stop()
    other = SpeechToTextNode("stt-own", {})
    await other.init()
    assert (await other.handle(wav_item(tone(0.2))))["text"] == "ok"


async def test_stt_local_backend(monkeypatch):
    monkeypatch.setitem(sys.modules, "faster_whisper", None)
    with pytest.raises(ValueError, match="pystreamflow\\[stt\\]"):
        await SpeechToTextNode("l", {"backend": "local"}).init()
    with pytest.raises(ValueError):
        await SpeechToTextNode("l", {"backend": "cloud"}).init()

    calls = []

    class FakeModel:
        def __init__(self, name, device, compute_type):
            calls.append((name, device, compute_type))

        def transcribe(self, audio, language=None, initial_prompt=None, beam_size=5):
            assert audio.read(4) == b"RIFF"
            seg = types.SimpleNamespace(start=0.0, end=0.5, text=" guten Tag ")
            return iter([seg]), types.SimpleNamespace(language=language or "de", duration=0.5)

    monkeypatch.setitem(sys.modules, "faster_whisper", types.SimpleNamespace(WhisperModel=FakeModel))
    import pystreamflow.nodes.speech_to_text as mod
    monkeypatch.setattr(mod, "_LOCAL_MODELS", {})
    for _ in range(2):
        node = SpeechToTextNode("local", {"backend": "local", "model": "tiny", "language": "de"})
        await node.init()
        details = await node.handle(wav_item(tone(0.5)))
        assert details["text"] == "guten Tag" and details["segments"][0]["end"] == 0.5
    assert calls == [("tiny", "auto", "default")]  # model loaded once


# --- registration ---------------------------------------------------------------------

def test_audio_nodes_registered_with_schemas():
    from pystreamflow.core.config_schema import get_field_schema
    from pystreamflow.core.port_schema import get_port_schema
    from pystreamflow.core.registry import build_node_registry

    reg = build_node_registry()
    for cls in (*an.AUDIO_NODE_CLASSES, SpeechToTextNode):
        assert reg[cls.__name__] is cls
    assert get_port_schema("AudioEncodeNode")["inputs"] == ["in", "flush"]
    assert get_port_schema("AudioLevelNode")["outputs"] == ["out", "rms_db", "peak_db"]
    assert get_port_schema("SpeechToTextNode")["outputs"] == ["out", "details", "errors"]
    assert "mp3" in get_field_schema("AudioEncodeNode")["format"]["options"]


def test_without_numpy_audio_nodes_are_unavailable():
    code = (
        "import sys, json; sys.modules['numpy'] = None\n"
        "from pystreamflow.core.registry import build_node_registry\n"
        "from pystreamflow.nodes import UNAVAILABLE_NODE_TYPES\n"
        "print(json.dumps({'registered': sorted(k for k in build_node_registry() if k.startswith(('Audio', 'Speech'))),"
        " 'unavailable': sorted(k for k in UNAVAILABLE_NODE_TYPES if k.startswith('Audio'))}))\n"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    result = json.loads(out.stdout.strip().splitlines()[-1])
    assert result["registered"] == ["SpeechToTextNode"]
    assert result["unavailable"] == sorted(c.__name__ for c in an.AUDIO_NODE_CLASSES)
