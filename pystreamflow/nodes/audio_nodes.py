"""Audio nodes (media plan phase 4), built on numpy + soundfile.

numpy and soundfile are optional (``pip install 'pystreamflow[audio]'``).
Like the image nodes, ``pystreamflow/nodes/__init__.py`` imports this
module inside ``try``/``except ImportError``; without them these types are
not registered and the editor greys them out.

Formats: soundfile (libsndfile >= 1.1) reads and writes WAV, FLAC, OGG
(Vorbis/Opus) and MP3. Anything else - AAC/M4A, the audio track of an
MP4/WebM, ... - is decoded through ffmpeg (``core/ffmpeg.py``) when it is
installed; ``AudioEncodeNode`` uses it for ``m4a``/``opus`` output.

How audio travels through a graph:

- ``kind: audio`` - a whole clip (an uploaded file, a finished segment).
- ``kind: audio_chunk`` - a slice of a longer stream, e.g. from
  ``AudioDecodeNode`` with ``chunk_ms``. Chunks are small WAV files
  (16-bit PCM), so each one is self-describing and previews in the
  editor's live view. Their ``meta`` carries ``sample_rate``,
  ``channels``, ``frames``, ``duration``, ``pts`` (seconds since the start
  of the stream), ``index`` and ``last`` (true on a stream's final chunk).

The processing nodes (resample, gain, normalize) always output 16-bit
PCM WAV, whatever came in - compress again with ``AudioEncodeNode``.
Everything runs in worker threads.
"""
from __future__ import annotations

import asyncio
import io
import math
import time
from typing import Any

import numpy as np
import soundfile as sf

from ..core.ffmpeg import FFmpegError
from ..core.ffmpeg import convert as ffmpeg_convert
from ..core.media import MediaItem, extension_for_mime
from ..core.media_transform import MediaTransformNode
from ..core.node import BaseNode

AUDIO_KINDS = ("audio", "audio_chunk")
DB_FLOOR = -120.0
# meta keys that describe one particular chunk and must not leak onto
# something built from several of them
_CHUNK_META = ("frames", "duration", "pts", "index", "last", "sample_rate", "channels", "format")

# format -> (soundfile format, subtype, MIME)
_SF_ENCODINGS = {
    "wav": ("WAV", "PCM_16", "audio/wav"),
    "flac": ("FLAC", "PCM_16", "audio/flac"),
    "ogg": ("OGG", "VORBIS", "audio/ogg"),
    "mp3": ("MP3", "MPEG_LAYER_III", "audio/mpeg"),
}
# format -> (file extension, ffmpeg codec args, MIME)
_FFMPEG_ENCODINGS = {
    "m4a": ("m4a", ["-c:a", "aac"], "audio/mp4"),
    "opus": ("opus", ["-c:a", "libopus"], "audio/ogg"),
}
ENCODE_FORMATS = tuple(_SF_ENCODINGS) + tuple(_FFMPEG_ENCODINGS)


# --- helpers --------------------------------------------------------------------

def _open_soundfile(item: MediaItem) -> sf.SoundFile:
    """Open an item for (block-wise) reading; ffmpeg fallback for formats
    libsndfile can't read."""
    data = item.get_bytes()
    try:
        return sf.SoundFile(io.BytesIO(data))
    except Exception as first:
        try:
            wav = ffmpeg_convert(data, "wav", ["-vn", "-c:a", "pcm_s16le"], input_ext=extension_for_mime(item.mime))
        except FFmpegError as e:
            raise ValueError(f"cannot decode {item.mime}: {first} (ffmpeg fallback: {e})") from None
        return sf.SoundFile(io.BytesIO(wav))


def decode_audio(item: MediaItem) -> tuple[np.ndarray, int]:
    """Whole item -> (float32 samples shaped (frames, channels), sample rate)."""
    with _open_soundfile(item) as f:
        return f.read(dtype="float32", always_2d=True), f.samplerate


def encode_audio(samples: np.ndarray, sample_rate: int, fmt: str = "wav", bitrate: str = "128k") -> tuple[bytes, str]:
    samples = np.clip(samples, -1.0, 1.0)
    if fmt in _SF_ENCODINGS:
        sf_format, subtype, mime = _SF_ENCODINGS[fmt]
        buf = io.BytesIO()
        try:
            sf.write(buf, samples, sample_rate, format=sf_format, subtype=subtype)
            return buf.getvalue(), mime
        except Exception:
            if fmt == "wav":
                raise
            # e.g. MP3 at a sample rate libsndfile's encoder rejects -
            # let ffmpeg resample and encode instead
            wav, _ = encode_audio(samples, sample_rate, "wav")
            return ffmpeg_convert(wav, fmt, ["-b:a", bitrate], input_ext="wav"), mime
    if fmt in _FFMPEG_ENCODINGS:
        ext, args, mime = _FFMPEG_ENCODINGS[fmt]
        wav, _ = encode_audio(samples, sample_rate, "wav")
        return ffmpeg_convert(wav, ext, [*args, "-b:a", bitrate], input_ext="wav"), mime
    raise ValueError(f"unsupported audio format {fmt!r}")


def audio_item(samples: np.ndarray, sample_rate: int, kind: str = "audio", fmt: str = "wav",
               meta: dict | None = None, bitrate: str = "128k") -> MediaItem:
    data, mime = encode_audio(samples, sample_rate, fmt, bitrate)
    frames = int(samples.shape[0])
    meta = {
        **(meta or {}),
        "sample_rate": int(sample_rate), "channels": int(samples.shape[1]),
        "frames": frames, "duration": round(frames / sample_rate, 6),
    }
    return MediaItem.from_bytes(data, kind=kind, mime=mime, meta=meta)


def source_meta(item: MediaItem) -> dict:
    """An item's meta without the per-chunk fields."""
    return {k: v for k, v in item.meta.items() if k not in _CHUNK_META}


def to_db(value: float) -> float:
    return max(DB_FLOOR, 20.0 * math.log10(value)) if value > 0 else DB_FLOOR


def levels(samples: np.ndarray) -> tuple[float, float]:
    """(rms, peak) over all channels, linear 0..1."""
    if samples.size == 0:
        return 0.0, 0.0
    return float(np.sqrt(np.mean(np.square(samples, dtype=np.float64)))), float(np.max(np.abs(samples)))


def window_db(samples: np.ndarray, window: int) -> np.ndarray:
    """RMS level in dB for each complete ``window``-frame slice."""
    n = samples.shape[0] // window
    if n == 0:
        return np.empty(0)
    frames = samples[: n * window].reshape(n, window, samples.shape[1]).astype(np.float64)
    rms = np.sqrt(np.mean(np.square(frames), axis=(1, 2)))
    with np.errstate(divide="ignore"):
        return np.maximum(DB_FLOOR, 20.0 * np.log10(rms))


def _lowpass(samples: np.ndarray, cutoff: float, taps: int = 63) -> np.ndarray:
    """Windowed-sinc FIR low-pass; ``cutoff`` in cycles per sample (< 0.5)."""
    n = np.arange(taps) - (taps - 1) / 2
    h = 2 * cutoff * np.sinc(2 * cutoff * n) * np.hamming(taps)
    h /= h.sum()
    return np.stack([np.convolve(samples[:, c], h, mode="same") for c in range(samples.shape[1])], axis=1)


def resample(samples: np.ndarray, sr_in: int, sr_out: int) -> np.ndarray:
    """Sample-rate conversion: anti-alias low-pass (when downsampling) plus
    linear interpolation. Good enough for speech and monitoring; each item
    is converted on its own, so chunk boundaries can carry a tiny click."""
    if sr_in == sr_out or samples.shape[0] == 0:
        return samples
    if sr_out < sr_in:
        samples = _lowpass(samples, 0.5 * sr_out / sr_in * 0.95)
    n_out = max(1, int(round(samples.shape[0] * sr_out / sr_in)))
    x_new = np.arange(n_out) * (sr_in / sr_out)
    x_old = np.arange(samples.shape[0])
    out = np.stack([np.interp(x_new, x_old, samples[:, c]) for c in range(samples.shape[1])], axis=1)
    return out.astype(np.float32)


def remix(samples: np.ndarray, channels: int) -> np.ndarray:
    have = samples.shape[1]
    if channels == have:
        return samples
    if channels == 1:
        return samples.mean(axis=1, keepdims=True)
    if have == 1:
        return np.repeat(samples, channels, axis=1)
    if channels < have:
        return samples[:, :channels]
    return np.concatenate([samples, np.repeat(samples[:, -1:], channels - have, axis=1)], axis=1)


def _float(config: dict, key: str, default: float) -> float:
    value = config.get(key, default)
    return default if value in (None, "") else float(value)


# --- per-item nodes -----------------------------------------------------------------

class _AudioTransform(MediaTransformNode):
    ACCEPT_KINDS = AUDIO_KINDS

    def process_samples(self, samples: np.ndarray, sr: int) -> tuple[np.ndarray, int]:
        raise NotImplementedError

    def transform_media(self, item):
        samples, sr = decode_audio(item)
        out, out_sr = self.process_samples(samples, sr)
        return audio_item(out, out_sr, item.kind, "wav", meta=dict(item.meta))


class AudioResampleNode(_AudioTransform):
    """Changes the sample rate and/or channel count.

    Config: ``sample_rate`` (Hz, e.g. 16000 for speech recognition; empty
    keeps it) and ``channels`` (1 = mono, 2 = stereo, empty/``keep``).
    """

    async def configure(self):
        sr = self.config.get("sample_rate", "")
        self.sample_rate = None if sr in (None, "", "keep") else int(sr)
        ch = self.config.get("channels", "keep")
        self.channels = None if ch in (None, "", "keep") else int(ch)
        if self.sample_rate is not None and not 1000 <= self.sample_rate <= 384000:
            raise ValueError("sample_rate must be between 1000 and 384000")
        if self.channels is not None and not 1 <= self.channels <= 8:
            raise ValueError("channels must be between 1 and 8")

    def process_samples(self, samples, sr):
        if self.channels:
            samples = remix(samples, self.channels)
        if self.sample_rate:
            return resample(samples, sr, self.sample_rate), self.sample_rate
        return samples, sr


class AudioGainNode(_AudioTransform):
    """Changes the level by ``gain_db`` decibels (default 0; +6 roughly
    doubles, -6 halves). Clips at full scale."""

    async def configure(self):
        self.gain_db = _float(self.config, "gain_db", 0.0)

    def process_samples(self, samples, sr):
        return samples * (10 ** (self.gain_db / 20)), sr


class AudioNormalizeNode(_AudioTransform):
    """Scales each item to a target level: ``mode`` peak (default, target
    ``target_db`` -1 dBFS) or rms (target default -20 dBFS). The gain is
    limited to ``max_gain_db`` (default 30) so near-silence isn't blown up
    to noise. Works per item - normalize whole clips or segments rather
    than short chunks, whose levels would be evened out."""

    async def configure(self):
        self.mode = str(self.config.get("mode", "peak")).lower()
        if self.mode not in ("peak", "rms"):
            raise ValueError("mode must be peak or rms")
        self.target_db = _float(self.config, "target_db", -1.0 if self.mode == "peak" else -20.0)
        self.max_gain_db = _float(self.config, "max_gain_db", 30.0)

    def process_samples(self, samples, sr):
        rms, peak = levels(samples)
        current = to_db(peak if self.mode == "peak" else rms)
        if current <= DB_FLOOR:
            return samples, sr
        gain_db = min(self.target_db - current, self.max_gain_db)
        return samples * (10 ** (gain_db / 20)), sr


class AudioLevelNode(MediaTransformNode):
    """Measures each audio item or chunk. ``out`` gets ``{rms, peak,
    rms_db, peak_db, silent, duration, pts}``; ``rms_db`` and ``peak_db``
    carry the bare numbers, ready for CompareNode or
    TriggerThresholdNode (e.g. silence or clipping detection). ``silent``
    is ``rms_db < silence_db`` (default -50). Levels are dBFS, floored at
    -120."""

    ACCEPT_KINDS = AUDIO_KINDS

    async def configure(self):
        self.silence_db = _float(self.config, "silence_db", -50.0)

    def transform_media(self, item):
        samples, sr = decode_audio(item)
        rms, peak = levels(samples)
        rms_db, peak_db = round(to_db(rms), 2), round(to_db(peak), 2)
        return {
            "rms": round(rms, 6), "peak": round(peak, 6), "rms_db": rms_db, "peak_db": peak_db,
            "silent": rms_db < self.silence_db, "duration": round(samples.shape[0] / sr, 6),
            "pts": item.meta.get("pts"),
        }

    def emit_result(self, item, result):
        self.emit('out', result)
        self.emit('rms_db', result['rms_db'])
        self.emit('peak_db', result['peak_db'])


# --- stream nodes -----------------------------------------------------------------

class AudioDecodeNode(BaseNode):
    """Decodes an audio file (or the audio track of a video, via ffmpeg)
    into PCM.

    Config:
      chunk_ms: 0 (default) emits the whole clip as one ``audio`` item
        (16-bit PCM WAV); > 0 splits it into ``audio_chunk`` items of that
        length, decoded block by block so a long file never has to fit in
        memory as raw samples. Chunks carry ``pts``/``index`` and ``last``
        on the final one.
    Anything that isn't audio/video passes through unchanged.
    """

    MEDIA_OUTPUT_PORTS = {"out": "block"}
    ACCEPT_KINDS = ("audio", "audio_chunk", "video")

    async def init(self):
        self.chunk_ms = _float(self.config, "chunk_ms", 0.0)
        if self.chunk_ms < 0:
            raise ValueError("chunk_ms must be >= 0")

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

    async def handle(self, item: Any) -> int:
        """Decode one item; returns how many items were emitted."""
        if not (isinstance(item, MediaItem) and item.kind in self.ACCEPT_KINDS):
            self.emit('out', item)
            return 1
        try:
            f = await asyncio.to_thread(_open_soundfile, item)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return 0
        meta = source_meta(item)
        count = 0
        try:
            with f:
                sr = f.samplerate
                if self.chunk_ms <= 0:
                    samples = await asyncio.to_thread(f.read, dtype="float32", always_2d=True)
                    out = await asyncio.to_thread(audio_item, samples, sr, "audio", "wav", {**meta, "pts": 0.0})
                    await self.emit_wait('out', out)
                    return 1
                block = max(1, int(round(sr * self.chunk_ms / 1000)))
                current = await asyncio.to_thread(f.read, block, "float32", True)
                index = 0
                while current.shape[0]:
                    following = await asyncio.to_thread(f.read, block, "float32", True)
                    chunk_meta = {**meta, "pts": round(index * block / sr, 6), "index": index,
                                  "last": following.shape[0] == 0}
                    out = await asyncio.to_thread(audio_item, current, sr, "audio_chunk", "wav", chunk_meta)
                    await self.emit_wait('out', out)
                    count += 1
                    index += 1
                    current = following
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # e.g. a file that is truncated half-way through
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
        return count


class _AudioBufferNode(BaseNode):
    """Shared buffering for nodes that collect audio across items
    (AudioEncodeNode, AudioSegmentNode).

    Incoming audio is appended to one sample buffer; a subclass cuts
    pieces off its front in ``on_append()`` and handles the remainder in
    ``on_flush()``. The buffer is flushed when: a whole ``audio`` item or
    a chunk marked ``last`` has been appended; the sample rate or channel
    count changes; anything arrives on the ``flush`` input port; or no
    input came for ``flush_idle_s`` seconds (default 2; 0 disables).
    Non-audio items pass through on ``out``.
    """

    async def configure(self):
        return None

    async def init(self):
        self.flush_idle_s = _float(self.config, "flush_idle_s", 2.0)
        self._samples: np.ndarray | None = None
        self._sr: int | None = None
        self._pts = 0.0          # stream time of the buffer's first sample
        self._meta: dict = {}
        self._last_input = 0.0
        self._flush_requested = False
        await self.configure()

    @property
    def buffered_frames(self) -> int:
        return 0 if self._samples is None else int(self._samples.shape[0])

    def take(self, frames: int) -> tuple[np.ndarray, float]:
        """Remove ``frames`` from the buffer's front: (samples, their pts)."""
        frames = min(frames, self.buffered_frames)
        piece, pts = self._samples[:frames], self._pts
        self._samples = self._samples[frames:]
        self._pts += frames / self._sr
        return piece, pts

    def make_item(self, samples: np.ndarray, pts: float, fmt: str = "wav", bitrate: str = "128k", **meta) -> MediaItem:
        return audio_item(samples, self._sr, "audio", fmt, {**self._meta, **meta, "pts": round(pts, 6)}, bitrate)

    async def on_append(self) -> None:
        return None

    async def on_flush(self) -> None:
        return None

    async def flush(self) -> None:
        self._flush_requested = False
        if self.buffered_frames:
            await self.on_flush()
        self._samples = None

    async def accept(self, item: Any) -> None:
        self._last_input = time.monotonic()
        if not (isinstance(item, MediaItem) and item.kind in AUDIO_KINDS):
            self.emit('out', item)
            return
        try:
            samples, sr = await asyncio.to_thread(decode_audio, item)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return
        if self.buffered_frames and (sr != self._sr or samples.shape[1] != self._samples.shape[1]):
            await self.flush()
        if not self.buffered_frames:
            self._sr = sr
            self._meta = source_meta(item)
            if isinstance(item.meta.get("pts"), (int, float)):
                self._pts = float(item.meta["pts"])
            elif self._samples is None:
                self._pts = 0.0
        self._samples = samples if not self.buffered_frames else np.concatenate([self._samples, samples])
        await self.on_append()
        if item.kind == "audio" or item.meta.get("last"):
            await self.flush()

    async def _flush_listener(self):
        while self._running:
            pipe = self.inputs.get('flush')
            if pipe is None:
                await asyncio.sleep(0.5)
                continue
            try:
                await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            self._flush_requested = True

    async def process(self):
        listener = asyncio.create_task(self._flush_listener())
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
                    await self.accept(item)
                    continue
                idle = self.flush_idle_s > 0 and time.monotonic() - self._last_input >= self.flush_idle_s
                if self.buffered_frames and (self._flush_requested or idle):
                    await self.flush()
                self._flush_requested = False
        finally:
            listener.cancel()


class AudioEncodeNode(_AudioBufferNode):
    """Collects audio and encodes it into a file-format item.

    Config:
      format: wav (default), flac, ogg (Vorbis), mp3; m4a (AAC) and opus
        need ffmpeg.
      bitrate: for mp3/m4a/opus via ffmpeg (default 128k).
      segment_s: > 0 emits a file every that many seconds of audio; 0
        (default) collects until a flush (see below).
      flush_idle_s: also emit what was collected after this many seconds
        without input (default 2; 0 = never).
    A whole ``audio`` item or a chunk marked ``last`` ends a file, as does
    any item on the ``flush`` input port.
    """

    async def configure(self):
        self.format = str(self.config.get("format", "wav")).lower()
        if self.format not in ENCODE_FORMATS:
            raise ValueError(f"format must be one of {', '.join(ENCODE_FORMATS)}")
        self.bitrate = str(self.config.get("bitrate", "128k"))
        self.segment_s = _float(self.config, "segment_s", 0.0)

    async def _emit_piece(self, samples, pts):
        try:
            item = await asyncio.to_thread(self.make_item, samples, pts, self.format, self.bitrate)
        except Exception as e:
            self._error_count += 1
            self._last_error = f"{type(e).__name__}: {e}"
            return
        self.emit('out', item)

    async def on_append(self):
        if self.segment_s > 0:
            size = max(1, int(round(self.segment_s * self._sr)))
            while self.buffered_frames >= size:
                await self._emit_piece(*self.take(size))

    async def on_flush(self):
        await self._emit_piece(*self.take(self.buffered_frames))


class AudioSegmentNode(_AudioBufferNode):
    """Cuts a stream into ``audio`` segments (WAV) with ``pts``.

    Config:
      mode: ``time`` - fixed ``segment_s`` pieces (default 10); or
        ``silence`` (default) - cut where the level stays below
        ``threshold_db`` (default -40 dBFS) for ``min_silence_ms`` (default
        500), judged in 20 ms windows (so cuts land within ~20 ms).
        Leading/trailing silence is trimmed to ``pad_ms`` (default 200); segments shorter than ``min_segment_s`` (default 0.3) are
        dropped as noise; a segment never gets longer than
        ``max_segment_s`` (default 30) - handy in front of speech
        recognition.
      flush_idle_s: see AudioEncodeNode.
    """

    WINDOW_MS = 20

    async def configure(self):
        c = self.config
        self.mode = str(c.get("mode", "silence")).lower()
        if self.mode not in ("time", "silence"):
            raise ValueError("mode must be time or silence")
        self.segment_s = _float(c, "segment_s", 10.0)
        self.threshold_db = _float(c, "threshold_db", -40.0)
        self.min_silence_ms = _float(c, "min_silence_ms", 500.0)
        self.pad_ms = _float(c, "pad_ms", 200.0)
        self.min_segment_s = _float(c, "min_segment_s", 0.3)
        self.max_segment_s = _float(c, "max_segment_s", 30.0)
        if self.segment_s <= 0 or self.max_segment_s <= 0:
            raise ValueError("segment_s and max_segment_s must be > 0")
        self._index = 0

    def _frames(self, ms: float, minimum: int = 1) -> int:
        return max(minimum, int(round(self._sr * ms / 1000)))

    async def _emit_segment(self, samples, pts):
        if samples.shape[0] == 0 or (self.mode == "silence" and samples.shape[0] / self._sr < self.min_segment_s):
            return
        item = await asyncio.to_thread(self.make_item, samples, pts, "wav", "128k", index=self._index)
        self._index += 1
        self.emit('out', item)

    async def on_append(self):
        if self.mode == "time":
            size = max(1, int(round(self.segment_s * self._sr)))
            while self.buffered_frames >= size:
                await self._emit_segment(*self.take(size))
            return
        win = self._frames(self.WINDOW_MS)
        pad = self._frames(self.pad_ms, 0)
        min_sil = max(1, int(round(self.min_silence_ms / self.WINDOW_MS)))
        max_frames = max(1, int(round(self.max_segment_s * self._sr)))
        while self.buffered_frames:
            voiced = window_db(self._samples, win) > self.threshold_db
            n = len(voiced)
            if n == 0:
                return
            if not voiced.any():
                # all silence so far: keep only the padding before whatever comes next
                self.take(max(0, n * win - pad))
                return
            first = int(np.argmax(voiced))
            if first * win > pad:
                self.take(first * win - pad)
                continue
            cut, run = None, 0
            for i in range(first, n):
                if voiced[i]:
                    run = 0
                    continue
                run += 1
                if run >= min_sil:
                    cut = i - run + 1
                    break
            if cut is not None:
                await self._emit_segment(*self.take(min(cut * win + pad, self.buffered_frames)))
                continue
            if self.buffered_frames >= max_frames:
                await self._emit_segment(*self.take(max_frames))
                continue
            return

    async def on_flush(self):
        if self.mode == "time":
            await self._emit_segment(*self.take(self.buffered_frames))
            return
        win = self._frames(self.WINDOW_MS)
        voiced = window_db(self._samples, win) > self.threshold_db
        # a trailing partial window counts as voiced if it is loud enough
        tail = self._samples[len(voiced) * win:]
        tail_voiced = tail.shape[0] > 0 and to_db(levels(tail)[0]) > self.threshold_db
        if tail_voiced:
            end = self.buffered_frames
        elif voiced.any():
            end = min(self.buffered_frames, (int(np.nonzero(voiced)[0][-1]) + 1) * win + self._frames(self.pad_ms, 0))
        else:
            return
        await self._emit_segment(*self.take(end))


AUDIO_NODE_CLASSES = (
    AudioDecodeNode, AudioEncodeNode, AudioResampleNode, AudioGainNode, AudioNormalizeNode,
    AudioLevelNode, AudioSegmentNode,
)
