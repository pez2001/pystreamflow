"""Media data type shared by every node that handles images, audio or video.

Phase 1 of the media plan (``docs/plans/media_types_plan.md``, 1.1): a
``MediaItem`` is the one envelope media travels through the graph in, so a
downstream node never has to guess what a bare ``bytes`` object is. It
carries the MIME type and whatever metadata the producer knows (width,
height, fps, sample rate, duration, pts, source path, ...) alongside the
payload.

The payload itself is either inline (``data``) or, above a size threshold,
parked in the disk-backed blob store (``ref``; see ``core/blob_store.py``)
so fan-out, ``BaseNode._last_items`` and the live view only ever copy a
short hash around. ``MediaItem.from_bytes()`` makes that choice; code
reading a payload calls ``get_bytes()`` (or ``await aget_bytes()`` from a
coroutine, which keeps the disk read off the event loop) and never has to
care which of the two it got.

``str(item)`` is a short description such as ``<image/png 1920x1080
3.1MB>``, so the existing text-oriented nodes (``DisplayNode``, the
``text_*`` family, templates, ...) print something readable rather than
megabytes of binary.

``to_jsonable()`` is the one place that turns anything a node may hold -
``MediaItem``, raw ``bytes``, and containers of them - into something JSON
can carry, for the live view, the reflection endpoints and the MCP tools.
"""
from __future__ import annotations

import asyncio
import mimetypes
import os
from dataclasses import dataclass, field
from typing import Any, Literal

from .blob_store import get_blob_store

MediaKind = Literal["image", "video", "audio", "video_frame", "audio_chunk", "binary"]
MEDIA_KINDS = ("image", "video", "audio", "video_frame", "audio_chunk", "binary")

# How many leading bytes of a binary payload the JSON summary shows as hex.
_HEAD_BYTES = 16


def inline_max_bytes() -> int:
    """Payloads up to this size stay inline in the item; bigger ones go to
    the blob store. ``PSF_MEDIA_INLINE_MAX_MB``, default 2 MB."""
    return int(float(os.environ.get("PSF_MEDIA_INLINE_MAX_MB", "2")) * 1024 * 1024)


def sniff_mime(data: bytes) -> str | None:
    """Guess a MIME type from a payload's magic bytes. Returns ``None`` if
    the format isn't recognized."""
    if not data:
        return None
    b = bytes(data[:64])
    if b.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if b.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if b.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if b.startswith(b"BM") and len(b) >= 14:
        return "image/bmp"
    if b.startswith((b"II*\x00", b"MM\x00*")):
        return "image/tiff"
    if b.startswith(b"RIFF") and len(b) >= 12:
        form = b[8:12]
        if form == b"WEBP":
            return "image/webp"
        if form == b"WAVE":
            return "audio/wav"
        if form == b"AVI ":
            return "video/x-msvideo"
        return None
    if b.startswith(b"OggS"):
        return "audio/ogg"
    if b.startswith(b"fLaC"):
        return "audio/flac"
    if b.startswith(b"ID3"):
        return "audio/mpeg"
    if len(b) >= 2 and b[0] == 0xFF and (b[1] & 0xE0) == 0xE0 and (b[1] & 0x06) == 0x02:
        # MPEG audio frame sync, layer III
        return "audio/mpeg"
    if len(b) >= 12 and b[4:8] == b"ftyp":
        brand = b[8:12]
        if brand in (b"M4A ", b"M4B "):
            return "audio/mp4"
        if brand == b"qt  ":
            return "video/quicktime"
        return "video/mp4"
    if b.startswith(b"\x1a\x45\xdf\xa3"):
        return "video/webm" if b"webm" in b else "video/x-matroska"
    return None


# mimetypes' own table is platform-dependent and misses or mislabels a
# few common media types; these always win.
_EXT_TO_MIME = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".gif": "image/gif",
    ".webp": "image/webp", ".bmp": "image/bmp", ".tif": "image/tiff", ".tiff": "image/tiff",
    ".wav": "audio/wav", ".mp3": "audio/mpeg", ".ogg": "audio/ogg", ".oga": "audio/ogg",
    ".flac": "audio/flac", ".m4a": "audio/mp4", ".aac": "audio/aac", ".opus": "audio/ogg",
    ".mp4": "video/mp4", ".m4v": "video/mp4", ".webm": "video/webm", ".mkv": "video/x-matroska",
    ".mov": "video/quicktime", ".avi": "video/x-msvideo",
}
_MIME_TO_EXT = {
    "image/jpeg": "jpg", "image/png": "png", "image/gif": "gif", "image/webp": "webp",
    "image/bmp": "bmp", "image/tiff": "tif", "audio/wav": "wav", "audio/x-wav": "wav",
    "audio/mpeg": "mp3", "audio/ogg": "ogg", "audio/flac": "flac", "audio/mp4": "m4a",
    "audio/aac": "aac", "video/mp4": "mp4", "video/webm": "webm", "video/x-matroska": "mkv",
    "video/quicktime": "mov", "video/x-msvideo": "avi", "application/octet-stream": "bin",
    "text/plain": "txt", "application/json": "json",
}


def mime_from_filename(filename: str | None) -> str | None:
    if not filename:
        return None
    ext = os.path.splitext(str(filename))[1].lower()
    return _EXT_TO_MIME.get(ext) or mimetypes.guess_type(str(filename))[0]


def guess_mime(data: bytes, filename: str | None = None) -> str:
    """Magic bytes first (they describe what the data really is), then the
    file extension, then ``application/octet-stream``."""
    return sniff_mime(data) or mime_from_filename(filename) or "application/octet-stream"


def is_media_filename(filename: str) -> bool:
    """True if the extension names an image, audio or video format."""
    return kind_for_mime(mime_from_filename(filename)) != "binary"


def extension_for_mime(mime: str | None) -> str:
    """File extension (without dot) for writing a payload of this type."""
    mime = (mime or "").split(";", 1)[0].strip().lower()
    if mime in _MIME_TO_EXT:
        return _MIME_TO_EXT[mime]
    ext = mimetypes.guess_extension(mime) if mime else None
    return ext.lstrip(".") if ext else "bin"


def kind_for_mime(mime: str | None) -> str:
    major = (mime or "").split("/", 1)[0]
    if major in ("image", "audio", "video"):
        return major
    return "binary"


def _format_size(n: int) -> str:
    if n < 1024:
        return f"{n}B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f}KB"
    if n < 1024 * 1024 * 1024:
        return f"{n / (1024 * 1024):.1f}MB"
    return f"{n / (1024 * 1024 * 1024):.1f}GB"


@dataclass
class MediaItem:
    kind: MediaKind
    mime: str                     # "image/png", "audio/wav", "video/mp4", ...
    data: bytes | None = None     # inline payload
    ref: str | None = None        # blob-store ref when the payload is stored out of line
    meta: dict = field(default_factory=dict)  # width/height/fps/sample_rate/channels/duration/pts/source_path
    _size: int | None = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if self.data is None and self.ref is None:
            raise ValueError("MediaItem needs either inline data or a blob ref")
        if self.kind not in MEDIA_KINDS:
            raise ValueError(f"unknown media kind {self.kind!r}")

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
        kind: str | None = None,
        mime: str | None = None,
        meta: dict | None = None,
        inline_limit: int | None = None,
    ) -> "MediaItem":
        """Wrap ``data``, sniffing ``mime`` (and deriving ``kind`` from it)
        when not given, and moving the payload to the blob store if it's
        larger than ``inline_limit`` (default: ``inline_max_bytes()``)."""
        data = bytes(data)
        mime = mime or sniff_mime(data) or "application/octet-stream"
        kind = kind or kind_for_mime(mime)
        limit = inline_max_bytes() if inline_limit is None else inline_limit
        if len(data) > limit:
            ref = get_blob_store().put(data)
            return cls(kind=kind, mime=mime, ref=ref, meta=dict(meta or {}), _size=len(data))
        return cls(kind=kind, mime=mime, data=data, meta=dict(meta or {}), _size=len(data))

    @classmethod
    async def afrom_bytes(cls, data: bytes, **kwargs) -> "MediaItem":
        """``from_bytes()`` with the hashing/disk write run in a thread."""
        return await asyncio.to_thread(cls.from_bytes, data, **kwargs)

    @property
    def is_inline(self) -> bool:
        return self.data is not None

    def get_bytes(self) -> bytes:
        """The payload, loaded from the blob store if stored out of line.
        Raises ``KeyError`` if the blob has already expired."""
        if self.data is not None:
            return self.data
        return get_blob_store().get(self.ref)

    async def aget_bytes(self) -> bytes:
        if self.data is not None:
            return self.data
        return await asyncio.to_thread(self.get_bytes)

    def size(self) -> int:
        if self.data is not None:
            return len(self.data)
        if self._size is None:
            try:
                self._size = get_blob_store().size(self.ref)
            except KeyError:
                return 0
        return self._size

    def ensure_ref(self) -> str:
        """Make sure the payload is in the blob store (so it can be served
        by ``GET /media/{ref}``) and return the ref. Inline data stays
        inline; the ref is only cached alongside it."""
        if self.ref is None:
            self.ref = get_blob_store().put(self.data)
        return self.ref

    def head(self, n: int = _HEAD_BYTES) -> bytes:
        if self.data is not None:
            return self.data[:n]
        try:
            return get_blob_store().read_head(self.ref, n)
        except KeyError:
            return b""

    def summary(self) -> dict:
        """JSON-safe description without the payload."""
        return {
            "$media": self.kind,
            "mime": self.mime,
            "size": self.size(),
            "ref": self.ref,
            "inline": self.is_inline,
            "meta": to_jsonable(self.meta),
        }

    def with_data(self, data: bytes, **changes) -> "MediaItem":
        """A new item with a different payload (inline/blob chosen again)
        and ``meta`` merged with ``changes.pop('meta', {})`` - the usual
        shape of a transform node's output."""
        meta = {**self.meta, **changes.pop("meta", {})}
        return MediaItem.from_bytes(
            data, kind=changes.pop("kind", self.kind), mime=changes.pop("mime", self.mime), meta=meta,
        )

    def __repr__(self) -> str:
        # Never the dataclass default, which would repr() the whole payload.
        return f"MediaItem{self}"

    def __str__(self) -> str:
        parts = [self.mime]
        w, h = self.meta.get("width"), self.meta.get("height")
        if w and h:
            parts.append(f"{w}x{h}")
        if self.meta.get("duration") is not None:
            parts.append(f"{float(self.meta['duration']):.2f}s")
        parts.append(_format_size(self.size()))
        return "<" + " ".join(parts) + ">"


def item_size(item: Any) -> int:
    """Byte size of an item for the ``bytes_in``/``bytes_out`` stats -
    without ever building ``str()`` of a binary payload, whose ``repr`` is
    about four times the payload's size."""
    if isinstance(item, MediaItem):
        return item.size()
    if isinstance(item, (bytes, bytearray)):
        return len(item)
    if isinstance(item, memoryview):
        return item.nbytes
    if isinstance(item, str):
        return len(item.encode("utf-8"))
    return len(str(item).encode("utf-8"))


def binary_summary(data: bytes | bytearray | memoryview) -> dict:
    """``{"$binary": size, "head": "<first bytes as hex>"}``."""
    view = memoryview(data)
    return {"$binary": view.nbytes, "head": view[:_HEAD_BYTES].tobytes().hex()}


def to_jsonable(obj: Any, media_url_prefix: str = "/media/", _depth: int = 0) -> Any:
    """Convert ``obj`` into something JSON can carry: ``bytes`` become
    ``{"$binary": size, "head": "89504e47..."}``, a ``MediaItem`` becomes
    its ``summary()`` plus a ``preview_url``, containers are converted
    recursively. Everything else is returned unchanged."""
    if _depth > 64:
        return str(obj)
    if isinstance(obj, MediaItem):
        out = obj.summary()
        try:
            out["ref"] = obj.ensure_ref()
            out["preview_url"] = f"{media_url_prefix}{out['ref']}?mime={obj.mime}"
        except Exception:
            out["preview_url"] = None
        return out
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return binary_summary(obj)
    if isinstance(obj, dict):
        return {k if isinstance(k, str) else str(k): to_jsonable(v, media_url_prefix, _depth + 1) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [to_jsonable(v, media_url_prefix, _depth + 1) for v in obj]
    return obj


def last_item_max_bytes() -> int:
    """Raw binary payloads larger than this are kept in a node's live-view
    history only as a summary. ``PSF_LAST_ITEM_MAX_BYTES``, default 64 KiB."""
    return int(os.environ.get("PSF_LAST_ITEM_MAX_BYTES", str(64 * 1024)))


def summarize_for_history(item: Any, limit: int, _depth: int = 0) -> Any:
    """What ``BaseNode._last_items`` keeps for ``item``: large raw binary
    payloads and large *inline* ``MediaItem`` payloads are replaced by a
    summary, so a node's history of up to ``max_last`` entries can't hold
    hundreds of megabytes. A ``MediaItem`` already stored by ref is kept
    as-is - it's small. Returns ``item`` itself when nothing needed
    replacing, so the common case costs no copy."""
    if _depth > 8:
        return item
    if isinstance(item, (bytes, bytearray, memoryview)):
        size = item.nbytes if isinstance(item, memoryview) else len(item)
        if size > limit:
            return {"kind": "binary", "size": size, "head": binary_summary(item)["head"]}
        return item
    if isinstance(item, MediaItem):
        if item.data is not None and len(item.data) > limit:
            return MediaItem(
                kind=item.kind, mime=item.mime, ref=item.ensure_ref(), meta=item.meta, _size=len(item.data),
            )
        return item
    if isinstance(item, dict):
        changed = False
        out = {}
        for k, v in item.items():
            nv = summarize_for_history(v, limit, _depth + 1)
            changed = changed or nv is not v
            out[k] = nv
        return out if changed else item
    if isinstance(item, list):
        new = [summarize_for_history(v, limit, _depth + 1) for v in item]
        return new if any(a is not b for a, b in zip(new, item)) else item
    return item
