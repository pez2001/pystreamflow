"""Video nodes (media plan phase 5).

Two backends, picked per node with ``backend: auto|pyav|ffmpeg``:

- **PyAV** (``pip install 'pystreamflow[video]'``) - the default when
  installed: frame-accurate decoding, ``pts`` per frame, keyframe flags,
  the audio track as a second stream, and encoding (H.264/VP9/...)
  in-process.
- **ffmpeg** subprocess (``core/ffmpeg.py``'s executable lookup) - the
  fallback: frames as an MJPEG pipe, thumbnails and encoding via temp
  files, info via ``ffprobe``. It can't emit the audio track or PNG
  frames.

Without either, these node types are not registered (greyed out in the
editor).

Frames travel as ``video_frame`` MediaItems holding a JPEG (default) or
PNG - an image, so every image node from phase 3 applies to them
directly - with ``meta``: ``pts`` (seconds), ``index``, ``fps``,
``width``, ``height``, ``keyframe`` (PyAV) and ``last`` on a finite
stream's final frame. A 1080p JPEG is ~200 KB instead of ~6 MB raw.
"""
from __future__ import annotations

import asyncio
import fractions
import io
import json
import os
import shutil
import subprocess
import tempfile
import time
import wave
from typing import Any, Iterator

from ..core.blob_store import get_blob_store
from ..core.ffmpeg import FFmpegError, ffmpeg_path
from ..core.media import MediaItem, extension_for_mime
from ..core.node import BaseNode

try:
    import av
    HAVE_PYAV = True
except ImportError:  # the ffmpeg fallback still works
    av = None
    HAVE_PYAV = False

VIDEO_KINDS = ("video",)
FRAME_FORMATS = ("jpeg", "png")
_EPS = 1e-6


def available() -> bool:
    return HAVE_PYAV or ffmpeg_path() is not None


def pick_backend(config: dict) -> str:
    choice = str(config.get("backend", "auto") or "auto").lower()
    if choice not in ("auto", "pyav", "ffmpeg"):
        raise ValueError("backend must be auto, pyav or ffmpeg")
    if choice in ("auto", "pyav") and HAVE_PYAV:
        return "pyav"
    if choice == "pyav":
        raise ValueError("backend 'pyav' needs PyAV: pip install 'pystreamflow[video]'")
    if ffmpeg_path() is None:
        raise ValueError("needs PyAV (pip install 'pystreamflow[video]') or the ffmpeg executable")
    return "ffmpeg"


def _quality_to_q(quality: int) -> int:
    """JPEG quality 1..100 -> ffmpeg's MJPEG qscale 31..2 (lower = better)."""
    return max(2, min(31, round(2 + (100 - quality) / 100 * 29)))


def _fit(width: int, height: int, max_side: int) -> tuple[int, int]:
    """Scale down to ``max_side`` (even sizes, as most codecs need)."""
    if not max_side or max(width, height) <= max_side:
        return width, height
    scale = max_side / max(width, height)
    return max(2, int(width * scale) // 2 * 2), max(2, int(height * scale) // 2 * 2)


class _MediaSource:
    """A MediaItem (or a path/URL) as something a decoder can open: the
    blob-store file for out-of-line items, else a temp file."""

    def __init__(self, item_or_url: Any):
        self._tmp = None
        if isinstance(item_or_url, MediaItem):
            item = item_or_url
            store = get_blob_store()
            if item.ref and store.exists(item.ref):
                self.path = store.path(item.ref)
            else:
                fd, self._tmp = tempfile.mkstemp(suffix="." + extension_for_mime(item.mime), prefix="psf-video-")
                with os.fdopen(fd, "wb") as f:
                    f.write(item.get_bytes())
                self.path = self._tmp
            self.meta = {k: v for k, v in item.meta.items() if k in ("filename", "source_path", "form")}
        else:
            self.path = str(item_or_url)
            self.meta = {"source": self.path}

    @property
    def is_stream(self) -> bool:
        return "://" in self.path and not self.path.startswith("file://")

    def close(self):
        if self._tmp:
            try:
                os.unlink(self._tmp)
            except OSError:
                pass
            self._tmp = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# --- PyAV helpers ---------------------------------------------------------------------

def _open_container(path: str, is_stream: bool):
    options = {"rtsp_transport": "tcp"} if path.startswith("rtsp") else {}
    return av.open(path, options=options, timeout=(10.0, 30.0) if is_stream else None)


class _FrameEncoder:
    """VideoFrame -> JPEG/PNG bytes through PyAV's own codecs (no Pillow)."""

    def __init__(self, fmt: str, quality: int):
        self.fmt = fmt
        self.q = _quality_to_q(quality)
        self._ctx = None
        self._size = None

    def encode(self, frame) -> bytes:
        size = (frame.width, frame.height)
        if self._ctx is None or self._size != size:
            codec, pix = ("mjpeg", "yuvj420p") if self.fmt == "jpeg" else ("png", "rgb24")
            ctx = av.CodecContext.create(codec, "w")
            ctx.width, ctx.height = size
            ctx.pix_fmt = pix
            ctx.time_base = fractions.Fraction(1, 25)
            if self.fmt == "jpeg":
                ctx.options = {"qmin": str(self.q), "qmax": str(self.q)}
            self._ctx, self._size = ctx, size
        packets = self._ctx.encode(frame.reformat(format=self._ctx.pix_fmt))
        if not packets:  # some encoders only emit on flush
            packets = self._ctx.encode(None)
            self._ctx = None
        return b"".join(bytes(p) for p in packets)


def _wav_bytes(pcm: bytes, rate: int, channels: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as w:
        w.setnchannels(channels)
        w.setsampwidth(2)
        w.setframerate(rate)
        w.writeframes(pcm)
    return buf.getvalue()


def iter_pyav(path: str, is_stream: bool, fps: float, max_side: int, fmt: str, quality: int,
              audio: bool, audio_chunk_ms: float, audio_rate: int) -> Iterator[tuple[str, bytes, dict]]:
    """Yields ('frame', image bytes, meta) and ('audio', wav bytes, meta)."""
    container = _open_container(path, is_stream)
    try:
        vstream = container.streams.video[0] if container.streams.video else None
        astream = container.streams.audio[0] if (audio and container.streams.audio) else None
        if vstream is None and astream is None:
            raise ValueError("no video stream found")
        streams = [s for s in (vstream, astream) if s is not None]
        if vstream is not None:
            vstream.thread_type = "AUTO"
        src_fps = float(vstream.average_rate or vstream.guessed_rate or 0) if vstream is not None else 0.0
        out_fps = fps if fps > 0 else src_fps
        encoder = _FrameEncoder(fmt, quality)
        next_t = None
        index = 0
        decoded = 0
        resampler = None
        pcm = bytearray()
        pcm_start = None
        a_rate = a_channels = None
        for packet in container.demux(*streams):
            for frame in packet.decode():
                if packet.stream is vstream:
                    t = frame.time if frame.time is not None else (decoded / src_fps if src_fps else float(decoded))
                    decoded += 1
                    if fps > 0 and next_t is not None and t < next_t - _EPS:
                        continue
                    if fps > 0:
                        # stay on a fixed grid (0, 1/fps, 2/fps, ...) so the
                        # sampling doesn't drift when frames don't line up
                        next_t = (next_t if next_t is not None else t) + 1.0 / fps
                        while next_t <= t + _EPS:
                            next_t += 1.0 / fps
                    w, h = _fit(frame.width, frame.height, max_side)
                    if (w, h) != (frame.width, frame.height):
                        frame = frame.reformat(width=w, height=h)
                    meta = {"pts": round(t, 6), "index": index, "fps": round(out_fps, 3) if out_fps else None,
                            "width": w, "height": h, "keyframe": bool(frame.key_frame)}
                    index += 1
                    yield "frame", encoder.encode(frame), meta
                else:
                    if resampler is None:
                        a_channels = 2 if frame.layout.nb_channels >= 2 else 1
                        a_rate = audio_rate or frame.sample_rate
                        resampler = av.AudioResampler(format="s16", layout="stereo" if a_channels == 2 else "mono", rate=a_rate)
                    if pcm_start is None:
                        pcm_start = frame.time or 0.0
                    for rf in resampler.resample(frame):
                        pcm.extend(bytes(rf.planes[0])[: rf.samples * a_channels * 2])
                    chunk_bytes = int(a_rate * audio_chunk_ms / 1000) * a_channels * 2
                    while chunk_bytes and len(pcm) >= chunk_bytes:
                        piece, pcm = bytes(pcm[:chunk_bytes]), pcm[chunk_bytes:]
                        yield "audio", _wav_bytes(piece, a_rate, a_channels), _audio_meta(pcm_start, piece, a_rate, a_channels)
                        pcm_start += chunk_bytes / (a_channels * 2) / a_rate
        if resampler is not None:
            for rf in resampler.resample(None):
                pcm.extend(bytes(rf.planes[0])[: rf.samples * a_channels * 2])
            if pcm:
                yield "audio", _wav_bytes(bytes(pcm), a_rate, a_channels), _audio_meta(pcm_start, pcm, a_rate, a_channels)
    finally:
        container.close()


def _audio_meta(start: float, pcm, rate: int, channels: int) -> dict:
    frames = len(pcm) // (channels * 2)
    return {"pts": round(start, 6), "sample_rate": rate, "channels": channels,
            "frames": frames, "duration": round(frames / rate, 6)}


# --- ffmpeg helpers -------------------------------------------------------------------

def ffprobe_path() -> str | None:
    exe = ffmpeg_path()
    if not exe:
        return None
    candidate = os.path.join(os.path.dirname(exe), "ffprobe" + (".exe" if exe.endswith(".exe") else ""))
    return candidate if os.path.isfile(candidate) else shutil.which("ffprobe")


def ffprobe(path: str, timeout: float = 60.0) -> dict:
    exe = ffprobe_path()
    if not exe:
        raise FFmpegError("ffprobe not found")
    proc = subprocess.run([exe, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", path],
                          capture_output=True, timeout=timeout)
    if proc.returncode != 0:
        raise FFmpegError(f"ffprobe failed: {proc.stderr.decode('utf-8', 'replace').strip()[-300:]}")
    return json.loads(proc.stdout or b"{}")


def _rate(value: str | None) -> float:
    try:
        num, _, den = str(value or "0/1").partition("/")
        return float(num) / float(den or 1)
    except (ValueError, ZeroDivisionError):
        return 0.0


def iter_ffmpeg(path: str, is_stream: bool, fps: float, max_side: int, quality: int) -> Iterator[tuple[str, bytes, dict]]:
    """Frames as an MJPEG pipe; pts derived from the output frame rate."""
    src_fps = 0.0
    if not is_stream:
        try:
            v = next((s for s in ffprobe(path).get("streams", []) if s.get("codec_type") == "video"), {})
            src_fps = _rate(v.get("avg_frame_rate")) or _rate(v.get("r_frame_rate"))
        except (FFmpegError, subprocess.TimeoutExpired, ValueError):
            pass
    out_fps = fps if fps > 0 else src_fps
    filters = []
    if fps > 0:
        filters.append(f"fps={fps}")
    if max_side:
        filters.append(f"scale='if(gt(iw,ih),min({max_side},iw),-2)':'if(gt(iw,ih),-2,min({max_side},ih))'")
    cmd = [ffmpeg_path(), "-hide_banner", "-loglevel", "error", "-nostdin"]
    if path.startswith("rtsp"):
        cmd += ["-rtsp_transport", "tcp"]
    cmd += ["-i", path, "-an"]
    if filters:
        cmd += ["-vf", ",".join(filters)]
    if fps <= 0:
        cmd += ["-fps_mode", "passthrough"]
    cmd += ["-f", "image2pipe", "-c:v", "mjpeg", "-q:v", str(_quality_to_q(quality)), "-"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    buf = b""
    index = 0
    try:
        while True:
            chunk = proc.stdout.read(1 << 16)
            if not chunk:
                break
            buf += chunk
            while True:
                start = buf.find(b"\xff\xd8")
                end = buf.find(b"\xff\xd9", start + 2) if start >= 0 else -1
                if start < 0 or end < 0:
                    break
                jpeg, buf = buf[start:end + 2], buf[end + 2:]
                w, h = _jpeg_size(jpeg)
                meta = {"pts": round(index / out_fps, 6) if out_fps else None, "index": index,
                        "fps": round(out_fps, 3) if out_fps else None, "width": w, "height": h}
                index += 1
                yield "frame", jpeg, meta
        proc.wait(timeout=10)
        if proc.returncode not in (0, None) and index == 0:
            raise FFmpegError(f"ffmpeg failed: {proc.stderr.read().decode('utf-8', 'replace').strip()[-300:]}")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def _jpeg_size(data: bytes) -> tuple[int | None, int | None]:
    """Width/height from a JPEG's SOF marker."""
    i = 2
    while i + 9 < len(data):
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        if marker in (0xC0, 0xC1, 0xC2):
            return int.from_bytes(data[i + 7:i + 9], "big"), int.from_bytes(data[i + 5:i + 7], "big")
        i += 2 + int.from_bytes(data[i + 2:i + 4], "big")
    return None, None


# --- nodes ------------------------------------------------------------------------------

def _int(config, key, default=0, minimum=0):
    value = config.get(key, default)
    value = default if value in (None, "") else int(float(value))
    if value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


def _float(config, key, default=0.0, minimum=0.0):
    value = config.get(key, default)
    value = default if value in (None, "") else float(value)
    if value < minimum:
        raise ValueError(f"{key} must be >= {minimum}")
    return value


class VideoDecodeNode(BaseNode):
    """Turns a video into a stream of ``video_frame`` items.

    Input: a ``video`` MediaItem on ``in`` (an upload, a MediaFileInputNode
    file), or ``source`` - a file path or a stream URL (``rtsp://``,
    ``http(s)://``, ...) opened when the node starts and whenever
    ``source`` is rewired.

    Config:
      fps: output frame rate (0 = every frame); the cheapest way to sample,
        since skipped frames are never encoded.
      max_side: scale frames down so the longer side is at most this many
        pixels (0 = original size).
      frame_format: jpeg (default) or png; quality 1-100 for JPEG (85).
      audio: also emit the audio track as ``audio_chunk`` WAV items on the
        ``audio`` port (PyAV backend only), ``audio_chunk_ms`` long (500)
        at ``audio_sample_rate`` (0 = original).
      reconnect_s: for stream URLs, reopen after this many seconds when the
        stream fails or ends (default 5; 0 = give up).
      backend: auto (PyAV if installed, else ffmpeg), pyav or ffmpeg.

    Both outputs use ``emit_wait()`` with a blocking bounded edge by
    default, so decoding a file never runs ahead of its consumers. For a
    live camera where old frames are worthless, give the edge
    ``buffer: {drop_policy: drop_oldest}`` instead.
    """

    MEDIA_OUTPUT_PORTS = {"out": "block", "audio": "block"}

    async def init(self):
        c = self.config
        self.backend = pick_backend(c)
        self.source = c.get("source") or None
        self.fps = _float(c, "fps", 0.0)
        self.max_side = _int(c, "max_side", 0)
        self.frame_format = str(c.get("frame_format", "jpeg") or "jpeg").lower()
        if self.frame_format not in FRAME_FORMATS:
            raise ValueError("frame_format must be jpeg or png")
        self.quality = _int(c, "quality", 85, minimum=1)
        if self.quality > 100:
            raise ValueError("quality must be <= 100")
        self.audio = bool(c.get("audio", False))
        self.audio_chunk_ms = _float(c, "audio_chunk_ms", 500.0, minimum=10.0)
        self.audio_sample_rate = _int(c, "audio_sample_rate", 0)
        self.reconnect_s = _float(c, "reconnect_s", 5.0)
        if self.backend == "ffmpeg" and (self.frame_format != "jpeg" or self.audio):
            raise ValueError("the ffmpeg backend supports only JPEG frames and no audio - install PyAV")

    def _iterator(self, src: _MediaSource):
        if self.backend == "pyav":
            return iter_pyav(src.path, src.is_stream, self.fps, self.max_side, self.frame_format,
                             self.quality, self.audio, self.audio_chunk_ms, self.audio_sample_rate)
        return iter_ffmpeg(src.path, src.is_stream, self.fps, self.max_side, self.quality)

    async def decode(self, source: Any) -> int:
        """Decode one item or path/URL completely (until it ends, or the
        node stops); returns the number of frames emitted."""
        mime = "image/jpeg" if self.frame_format == "jpeg" else "image/png"
        frames = 0
        with _MediaSource(source) as src:
            gen = await asyncio.to_thread(self._iterator, src)
            pending = None
            try:
                while self._running:
                    step = await asyncio.to_thread(next, gen, None)
                    if step is None:
                        break
                    kind, data, meta = step
                    if kind == "audio":
                        await self.emit_wait('audio', MediaItem.from_bytes(
                            data, kind="audio_chunk", mime="audio/wav", meta={**src.meta, **meta}))
                        continue
                    item = MediaItem.from_bytes(data, kind="video_frame", mime=mime, meta={**src.meta, **meta, "last": False})
                    if pending is not None:
                        await self.emit_wait('out', pending)
                        frames += 1
                    pending = item
                if pending is not None:
                    pending.meta["last"] = not src.is_stream or not self._running
                    await self.emit_wait('out', pending)
                    frames += 1
            finally:
                await asyncio.to_thread(gen.close)
        return frames

    async def _decode_safely(self, source) -> bool:
        try:
            await self.decode(source)
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return False

    async def process(self):
        done_source = None
        while self._running:
            source = self.source
            if source and source != done_source:
                is_stream = "://" in str(source) and not str(source).startswith("file://")
                ok = await self._decode_safely(source)
                if is_stream and self.reconnect_s > 0 and self._running and source == self.source:
                    await asyncio.sleep(self.reconnect_s)   # stream ended or failed: reopen
                    continue
                if not ok and not is_stream:
                    await asyncio.sleep(1.0)
                done_source = source
                continue
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if isinstance(item, MediaItem) and item.kind in VIDEO_KINDS:
                await self._decode_safely(item)
            else:
                self.emit('out', item)


class VideoFrameSampleNode(BaseNode):
    """Thins out a frame stream.

    Config ``mode``:
      every_n (default) - keep every ``n``-th frame (n default 5);
      fps - keep at most ``fps`` frames per second of stream time (by
        ``pts``; default 1);
      keyframes - keep only frames the decoder flagged as keyframes
        (PyAV backend).
    The first frame of a stream is always kept; a frame whose ``index``
    goes backwards (a new video) restarts the count. Non-frame items pass
    through.
    """

    async def init(self):
        c = self.config
        self.mode = str(c.get("mode", "every_n")).lower()
        if self.mode not in ("every_n", "fps", "keyframes"):
            raise ValueError("mode must be every_n, fps or keyframes")
        self.n = _int(c, "n", 5, minimum=1)
        self.target_fps = _float(c, "fps", 1.0)
        if self.mode == "fps" and self.target_fps <= 0:
            raise ValueError("fps must be > 0")
        self._count = 0
        self._next_pts = None
        self._last_index = None

    def keep(self, item: MediaItem) -> bool:
        index = item.meta.get("index")
        if isinstance(index, int) and self._last_index is not None and index <= self._last_index:
            self._count, self._next_pts = 0, None
        self._last_index = index if isinstance(index, int) else self._last_index
        if self.mode == "keyframes":
            return bool(item.meta.get("keyframe"))
        if self.mode == "every_n":
            keep = self._count % self.n == 0
            self._count += 1
            return keep
        pts = item.meta.get("pts")
        if not isinstance(pts, (int, float)):
            keep = self._count % max(1, round((item.meta.get("fps") or self.target_fps) / self.target_fps)) == 0
            self._count += 1
            return keep
        if self._next_pts is None or pts >= self._next_pts - _EPS:
            base = pts if self._next_pts is None else self._next_pts
            self._next_pts = base + 1.0 / self.target_fps
            while self._next_pts <= pts:
                self._next_pts += 1.0 / self.target_fps
            return True
        return False

    async def process(self):
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not (isinstance(item, MediaItem) and item.kind == "video_frame"):
                self.emit('out', item)
            elif self.keep(item):
                self.emit('out', item)


def _probe_info(path: str, backend: str) -> dict:
    if backend == "pyav":
        container = av.open(path)
        try:
            info: dict[str, Any] = {
                "container": container.format.name,
                "duration": round(container.duration / 1_000_000, 3) if container.duration else None,
                "bit_rate": container.bit_rate or None,
            }
            if container.streams.video:
                v = container.streams.video[0]
                info.update({
                    "video_codec": v.codec_context.name, "width": v.width, "height": v.height,
                    "fps": round(float(v.average_rate), 3) if v.average_rate else None,
                    "frames": v.frames or None, "pix_fmt": v.codec_context.pix_fmt,
                })
            if container.streams.audio:
                a = container.streams.audio[0]
                info.update({"audio_codec": a.codec_context.name, "sample_rate": a.sample_rate,
                             "channels": a.codec_context.channels if hasattr(a.codec_context, "channels") else a.layout.nb_channels})
            return info
        finally:
            container.close()
    data = ffprobe(path)
    fmt = data.get("format", {})
    info = {"container": fmt.get("format_name"),
            "duration": round(float(fmt["duration"]), 3) if fmt.get("duration") else None,
            "bit_rate": int(fmt["bit_rate"]) if fmt.get("bit_rate") else None}
    for s in data.get("streams", []):
        if s.get("codec_type") == "video" and "video_codec" not in info:
            info.update({"video_codec": s.get("codec_name"), "width": s.get("width"), "height": s.get("height"),
                         "fps": round(_rate(s.get("avg_frame_rate")), 3) or None,
                         "frames": int(s["nb_frames"]) if s.get("nb_frames", "").isdigit() else None,
                         "pix_fmt": s.get("pix_fmt")})
        elif s.get("codec_type") == "audio" and "audio_codec" not in info:
            info.update({"audio_codec": s.get("codec_name"),
                         "sample_rate": int(s["sample_rate"]) if s.get("sample_rate") else None,
                         "channels": s.get("channels")})
    return info


class VideoInfoNode(BaseNode):
    """Emits a dict describing a ``video`` item: ``container``,
    ``duration`` (s), ``bit_rate``, ``video_codec``, ``width``,
    ``height``, ``fps``, ``frames``, ``pix_fmt`` and, if there is one,
    ``audio_codec``, ``sample_rate``, ``channels`` - plus ``mime``,
    ``size`` and the item's ``filename``. Non-video items pass through."""

    async def init(self):
        self.backend = pick_backend(self.config)

    async def handle(self, item):
        if not (isinstance(item, MediaItem) and item.kind in VIDEO_KINDS):
            self.emit('out', item)
            return item

        def work():
            with _MediaSource(item) as src:
                return _probe_info(src.path, self.backend)

        try:
            info = await asyncio.to_thread(work)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return None
        info = {"mime": item.mime, "size": item.size(), "filename": item.meta.get("filename"), **info}
        self.emit('out', info)
        return info

    async def process(self):
        while self._running:
            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self.handle(item)


class VideoThumbnailNode(VideoInfoNode):
    """Grabs one frame of a ``video`` item as an ``image`` item (JPEG):
    at ``at_s`` seconds (default 1.0) or, if set, at ``at_percent`` of the
    duration; clamped to the video's end. ``max_side`` scales it down
    (default 0 = original size); ``quality`` 1-100 (85)."""

    async def init(self):
        await super().init()
        c = self.config
        self.at_s = _float(c, "at_s", 1.0)
        pct = c.get("at_percent")
        self.at_percent = None if pct in (None, "") else float(pct)
        if self.at_percent is not None and not 0 <= self.at_percent <= 100:
            raise ValueError("at_percent must be between 0 and 100")
        self.max_side = _int(c, "max_side", 0)
        self.quality = _int(c, "quality", 85, minimum=1)

    def _grab(self, path: str) -> tuple[bytes, dict]:
        duration = None
        try:
            duration = _probe_info(path, self.backend).get("duration")
        except Exception:
            pass
        t = self.at_s
        if self.at_percent is not None and duration:
            t = duration * self.at_percent / 100
        if duration:
            t = max(0.0, min(t, duration - 0.05))
        if self.backend == "pyav":
            container = av.open(path)
            try:
                stream = container.streams.video[0]
                if t > 0 and stream.time_base:
                    container.seek(int(t / stream.time_base), stream=stream, backward=True, any_frame=False)
                chosen = None
                for frame in container.decode(stream):
                    chosen = frame
                    if frame.time is None or frame.time >= t - _EPS:
                        break
                if chosen is None:
                    raise ValueError("no frame decoded")
                w, h = _fit(chosen.width, chosen.height, self.max_side)
                if (w, h) != (chosen.width, chosen.height):
                    chosen = chosen.reformat(width=w, height=h)
                return _FrameEncoder("jpeg", self.quality).encode(chosen), {
                    "width": w, "height": h, "source_pts": round(chosen.time or 0.0, 6)}
            finally:
                container.close()
        with tempfile.TemporaryDirectory(prefix="psf-thumb-") as tmp:
            out = os.path.join(tmp, "thumb.jpg")
            vf = ["-vf", f"scale='if(gt(iw,ih),min({self.max_side},iw),-2)':'if(gt(iw,ih),-2,min({self.max_side},ih))'"] if self.max_side else []
            q = ["-q:v", str(_quality_to_q(self.quality))]
            cmd = [ffmpeg_path(), "-hide_banner", "-loglevel", "error", "-nostdin", "-ss", f"{t:.3f}", "-i", path,
                   "-frames:v", "1", *vf, *q, "-y", out]
            proc = subprocess.run(cmd, capture_output=True, timeout=120)
            if proc.returncode == 0 and not os.path.exists(out):
                # past the last video frame (the container's duration can
                # include a longer audio track): take the last frame instead
                cmd = [ffmpeg_path(), "-hide_banner", "-loglevel", "error", "-nostdin", "-sseof", "-1", "-i", path,
                       *vf, "-update", "1", *q, "-y", out]
                proc = subprocess.run(cmd, capture_output=True, timeout=120)
            if proc.returncode != 0 or not os.path.exists(out):
                raise FFmpegError(f"ffmpeg failed: {proc.stderr.decode('utf-8', 'replace').strip()[-300:]}")
            data = open(out, "rb").read()
            w, h = _jpeg_size(data)
            return data, {"width": w, "height": h, "source_pts": round(t, 6)}

    async def handle(self, item):
        if not (isinstance(item, MediaItem) and item.kind in VIDEO_KINDS):
            self.emit('out', item)
            return item

        def work():
            with _MediaSource(item) as src:
                return self._grab(src.path)

        try:
            data, meta = await asyncio.to_thread(work)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return None
        stem = os.path.splitext(str(item.meta.get("filename") or "video"))[0]
        out = MediaItem.from_bytes(data, kind="image", mime="image/jpeg",
                                   meta={"filename": f"{stem}.jpg", **meta})
        self.emit('out', out)
        return out


# --- encoding ---------------------------------------------------------------------------

_CONTAINERS = {
    # format: (mime, video codec candidates, audio codec, pix_fmt)
    "mp4": ("video/mp4", ("libx264", "h264", "mpeg4"), "aac", "yuv420p"),
    "webm": ("video/webm", ("libvpx-vp9", "libvpx"), "libopus", "yuv420p"),
}


def _decode_image(data: bytes):
    """JPEG/PNG bytes -> PyAV VideoFrame."""
    with av.open(io.BytesIO(data)) as c:
        return next(c.decode(video=0))


class _PyAVSegment:
    """One output file being written incrementally (so a long segment never
    has to sit in memory as frames)."""

    def __init__(self, fmt: str, fps: float, width: int, height: int, with_audio: bool,
                 audio_rate: int, audio_channels: int, crf: int):
        mime, codecs, audio_codec, pix_fmt = _CONTAINERS[fmt]
        fd, self.path = tempfile.mkstemp(suffix="." + fmt, prefix="psf-enc-")
        os.close(fd)
        self.mime = mime
        self.container = av.open(self.path, "w", format=fmt)
        codec = next((c for c in codecs if c in av.codecs_available), codecs[-1])
        self.vstream = self.container.add_stream(codec, rate=fractions.Fraction(fps).limit_denominator(1001))
        self.vstream.width, self.vstream.height = width, height
        self.vstream.pix_fmt = pix_fmt
        self.vstream.codec_context.time_base = fractions.Fraction(1, 1000)
        # constant-quality mode: VP9 needs b=0 for pure CRF, VP8 a bitrate cap
        options = {"libx264": {"crf": str(crf)}, "libvpx-vp9": {"crf": str(crf), "b": "0"},
                   "libvpx": {"crf": str(crf), "b": "1M"}}.get(codec)
        if options:
            self.vstream.codec_context.options = options
        self.astream = None
        self.resampler = None
        if with_audio:
            layout = "stereo" if audio_channels >= 2 else "mono"
            self.astream = self.container.add_stream(audio_codec, rate=48000 if audio_codec == "libopus" else audio_rate)
            self.astream.layout = layout
            # AAC/Opus encoders only accept fixed-size frames
            self.resampler = av.AudioResampler(format=self.astream.codec_context.format.name, layout=layout,
                                               rate=self.astream.codec_context.sample_rate,
                                               frame_size=960 if audio_codec == "libopus" else 1024)
        self.size = (width, height)
        self.last_pts = -1
        self.frames = 0
        self.first_t = None
        self.last_t = 0.0
        self.audio_samples = 0

    def add_frame(self, data: bytes, t: float):
        frame = _decode_image(data)
        if (frame.width, frame.height) != self.size:
            frame = frame.reformat(width=self.size[0], height=self.size[1])
        frame = frame.reformat(format=self.vstream.pix_fmt)
        if self.first_t is None:
            self.first_t = t
        pts = max(self.last_pts + 1, int(round((t - self.first_t) * 1000)))
        frame.pts, frame.time_base = pts, fractions.Fraction(1, 1000)
        self.last_pts = pts
        self.last_t = t
        self.frames += 1
        for packet in self.vstream.encode(frame):
            self.container.mux(packet)

    def add_audio(self, wav: bytes):
        if self.astream is None:
            return
        with av.open(io.BytesIO(wav)) as c:
            for frame in c.decode(audio=0):
                frame.pts = None
                for rf in self.resampler.resample(frame):
                    rf.pts = self.audio_samples
                    self.audio_samples += rf.samples
                    for packet in self.astream.encode(rf):
                        self.container.mux(packet)

    def finish(self) -> tuple[bytes, str]:
        try:
            for packet in self.vstream.encode(None):
                self.container.mux(packet)
            if self.astream is not None:
                for rf in self.resampler.resample(None):
                    rf.pts = self.audio_samples
                    self.audio_samples += rf.samples
                    for packet in self.astream.encode(rf):
                        self.container.mux(packet)
                for packet in self.astream.encode(None):
                    self.container.mux(packet)
            self.container.close()
            with open(self.path, "rb") as f:
                return f.read(), self.mime
        finally:
            self.discard()

    def discard(self):
        try:
            self.container.close()
        except Exception:
            pass
        try:
            os.unlink(self.path)
        except OSError:
            pass


class _FFmpegSegment:
    """Fallback encoder: frames and audio collected in a temp dir, muxed by
    ffmpeg on finish (concat demuxer with per-frame durations)."""

    def __init__(self, fmt: str, fps: float, width: int, height: int, with_audio: bool,
                 audio_rate: int, audio_channels: int, crf: int):
        self.fmt, self.fps, self.crf = fmt, fps, crf
        self.size = (width, height)
        self.mime = _CONTAINERS[fmt][0]
        self.dir = tempfile.mkdtemp(prefix="psf-enc-")
        self.frame_times: list[tuple[str, float]] = []
        self.with_audio = with_audio
        self.audio = bytearray()
        self.audio_params = None
        self.frames = 0
        self.first_t = None
        self.last_t = 0.0

    def add_frame(self, data: bytes, t: float):
        name = os.path.join(self.dir, f"f{self.frames:07d}.img")
        with open(name, "wb") as f:
            f.write(data)
        if self.first_t is None:
            self.first_t = t
        self.frame_times.append((name, t))
        self.last_t = t
        self.frames += 1

    def add_audio(self, wav: bytes):
        if not self.with_audio:
            return
        with wave.open(io.BytesIO(wav)) as w:
            params = (w.getnchannels(), w.getsampwidth(), w.getframerate())
            if self.audio_params is None:
                self.audio_params = params
            if params == self.audio_params:
                self.audio.extend(w.readframes(w.getnframes()))

    def finish(self) -> tuple[bytes, str]:
        try:
            lines = []
            for i, (name, t) in enumerate(self.frame_times):
                nxt = self.frame_times[i + 1][1] if i + 1 < len(self.frame_times) else t + 1.0 / self.fps
                lines.append(f"file '{name}'\nduration {max(0.001, nxt - t):.6f}")
            lines.append(f"file '{self.frame_times[-1][0]}'")
            listing = os.path.join(self.dir, "frames.txt")
            with open(listing, "w") as f:
                f.write("\n".join(lines) + "\n")
            cmd = [ffmpeg_path(), "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                   "-f", "concat", "-safe", "0", "-i", listing]
            if self.with_audio and self.audio:
                wav = os.path.join(self.dir, "audio.wav")
                ch, width, rate = self.audio_params
                with wave.open(wav, "wb") as w:
                    w.setnchannels(ch)
                    w.setsampwidth(width)
                    w.setframerate(rate)
                    w.writeframes(bytes(self.audio))
                cmd += ["-i", wav]
            w_, h_ = self.size
            # the concat demuxer needs the last file listed twice; -frames:v
            # keeps that from becoming an extra frame
            cmd += ["-vf", f"scale={w_}:{h_},format=yuv420p", "-fps_mode", "vfr", "-frames:v", str(self.frames)]
            if self.fmt == "mp4":
                cmd += ["-c:v", "libx264", "-crf", str(self.crf), "-c:a", "aac", "-movflags", "+faststart"]
            else:
                cmd += ["-c:v", "libvpx-vp9", "-crf", str(self.crf), "-b:v", "0", "-c:a", "libopus"]
            out = os.path.join(self.dir, f"out.{self.fmt}")
            proc = subprocess.run([*cmd, out], capture_output=True, timeout=600)
            if proc.returncode != 0:
                raise FFmpegError(f"ffmpeg failed: {proc.stderr.decode('utf-8', 'replace').strip()[-300:]}")
            with open(out, "rb") as f:
                return f.read(), self.mime
        finally:
            self.discard()

    def discard(self):
        shutil.rmtree(self.dir, ignore_errors=True)


class VideoEncodeNode(BaseNode):
    """Encodes a stream of frames (``video_frame`` or ``image`` items on
    ``in``), optionally with sound (``audio``/``audio_chunk`` items on the
    ``audio`` port), into a ``video`` item.

    Config:
      format: mp4 (H.264 + AAC, default) or webm (VP9 + Opus).
      fps: nominal rate (default: the frames' own ``fps`` meta, else 25);
        timing follows the frames' ``pts``, so a sampled 1 fps stream
        plays back in real time.
      crf: quality, lower is better (default 23 for mp4, 32 for webm).
      segment_s: > 0 starts a new file every that many seconds of stream
        time; 0 (default) until a flush.
      flush_idle_s: finish after this many seconds without input (default
        2; 0 = never).
    A file is also finished on a frame marked ``last`` (with the ``audio``
    port wired, ``audio_grace_s`` later - default 0.5 - so sound that lags
    behind the frames still makes it in), on anything on the ``flush``
    port, and when the node stops. All frames of a file take the
    first frame's size. Audio is included when the ``audio`` port is wired
    at the moment a file starts.
    """

    async def init(self):
        c = self.config
        self.backend = pick_backend(c)
        self.format = str(c.get("format", "mp4")).lower()
        if self.format not in _CONTAINERS:
            raise ValueError("format must be mp4 or webm")
        self.fps = _float(c, "fps", 0.0)
        self.crf = _int(c, "crf", 23 if self.format == "mp4" else 32)
        self.segment_s = _float(c, "segment_s", 0.0)
        self.flush_idle_s = _float(c, "flush_idle_s", 2.0)
        self.audio_grace_s = _float(c, "audio_grace_s", 0.5)
        self._finish_at: float | None = None
        self._segment = None
        self._segment_meta: dict = {}
        self._last_input = 0.0
        self._flush_requested = False
        self._audio_backlog: list[bytes] = []
        # Frames (main loop) and audio (its own listener task) must never
        # touch the same encoder/container from two worker threads at once
        # - PyAV isn't thread-safe per container.
        self._lock = asyncio.Lock()

    def _new_segment(self, item: MediaItem, data: bytes):
        fps = self.fps or float(item.meta.get("fps") or 0) or 25.0
        width, height = item.meta.get("width"), item.meta.get("height")
        if not (width and height):
            if self.backend == "pyav":
                frame = _decode_image(data)
                width, height = frame.width, frame.height
            else:
                width, height = _jpeg_size(data)
        width, height = max(2, int(width) // 2 * 2), max(2, int(height) // 2 * 2)
        with_audio = self.inputs.get('audio') is not None
        cls = _PyAVSegment if self.backend == "pyav" else _FFmpegSegment
        return cls(self.format, fps, width, height, with_audio, 48000, 2, self.crf)

    async def add_frame(self, item: MediaItem):
        async with self._lock:
            await self._add_frame(item)

    async def _add_frame(self, item: MediaItem):
        self._last_input = time.monotonic()
        data = await item.aget_bytes()
        t = item.meta.get("pts")
        if not isinstance(t, (int, float)):
            t = (self._segment.last_t + 1.0 / (self._segment_meta.get("fps") or 25.0)) if self._segment else 0.0
        if self._segment is not None and (self._finish_at is not None or (
                self.segment_s > 0 and t - self._segment.first_t >= self.segment_s - _EPS)):
            await self._finish()
        if self._segment is None:
            self._segment = await asyncio.to_thread(self._new_segment, item, data)
            self._segment_meta = {"pts": round(t, 6), "fps": self._segment_fps(item),
                                  **{k: item.meta[k] for k in ("filename", "source", "source_path") if k in item.meta}}
            for wav in self._audio_backlog:
                await asyncio.to_thread(self._segment.add_audio, wav)
            self._audio_backlog.clear()
        await asyncio.to_thread(self._segment.add_frame, data, float(t))
        if item.meta.get("last"):
            if getattr(self._segment, "astream", None) is not None or getattr(self._segment, "with_audio", False):
                # audio arrives on its own port and may lag behind the frames
                self._finish_at = time.monotonic() + self.audio_grace_s
            else:
                await self._finish()

    def _segment_fps(self, item):
        return self.fps or float(item.meta.get("fps") or 0) or 25.0

    async def add_audio(self, item: MediaItem):
        wav = await item.aget_bytes()
        if item.mime != "audio/wav":
            try:
                from ..core.ffmpeg import convert
                wav = await asyncio.to_thread(convert, wav, "wav", ["-c:a", "pcm_s16le"], extension_for_mime(item.mime))
            except FFmpegError as e:
                self._error_count += 1
                self._last_error = f"audio: {e}"
                return
        async with self._lock:
            if self._segment is None:
                self._audio_backlog.append(wav)
            else:
                await asyncio.to_thread(self._segment.add_audio, wav)

    async def finish(self) -> MediaItem | None:
        async with self._lock:
            return await self._finish()

    async def _finish(self) -> MediaItem | None:
        self._flush_requested = False
        self._finish_at = None
        segment, self._segment = self._segment, None
        if segment is None:
            return None
        try:
            data, mime = await asyncio.to_thread(segment.finish)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return None
        fps = self._segment_meta.get("fps") or 25.0
        meta = {**self._segment_meta, "frames": segment.frames, "width": segment.size[0], "height": segment.size[1],
                "duration": round(segment.last_t - segment.first_t + 1.0 / fps, 6)}
        stem = os.path.splitext(str(meta.pop("filename", "") or "video"))[0]
        meta["filename"] = f"{stem}.{self.format}"
        item = await MediaItem.afrom_bytes(data, kind="video", mime=mime, meta=meta)
        self.emit('out', item)
        return item

    async def _side_listener(self, port: str):
        while self._running:
            pipe = self.inputs.get(port)
            if pipe is None:
                await asyncio.sleep(0.5)
                continue
            try:
                value = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if port == 'flush':
                self._flush_requested = True
            elif isinstance(value, MediaItem) and value.kind in ("audio", "audio_chunk"):
                await self.add_audio(value)

    async def process(self):
        listeners = [asyncio.create_task(self._side_listener(p)) for p in ('flush', 'audio')]
        try:
            while self._running:
                pipe = self.inputs.get('in')
                item = None
                if pipe is None:
                    await asyncio.sleep(0.2)
                else:
                    try:
                        item = await asyncio.wait_for(pipe.get(), timeout=0.2)
                    except asyncio.TimeoutError:
                        pass
                if item is not None:
                    if isinstance(item, MediaItem) and item.kind in ("video_frame", "image"):
                        try:
                            await self.add_frame(item)
                        except asyncio.CancelledError:
                            raise
                        except Exception as e:
                            self._error_count += 1
                            self._last_error = f"{type(e).__name__}: {e}"
                    else:
                        self.emit('out', item)
                    continue
                now = time.monotonic()
                idle = self.flush_idle_s > 0 and now - self._last_input >= self.flush_idle_s
                graced = self._finish_at is not None and now >= self._finish_at
                if self._segment is not None and (self._flush_requested or idle or graced):
                    await self.finish()
                self._flush_requested = False
        finally:
            for t in listeners:
                t.cancel()
            if self._segment is not None:
                await asyncio.shield(self.finish())


VIDEO_NODE_CLASSES = (VideoDecodeNode, VideoFrameSampleNode, VideoEncodeNode, VideoInfoNode, VideoThumbnailNode)
