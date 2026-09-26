"""Media support for the MCP tools (media plan phase 7).

- ``resolve_media_payload()`` lets ``send_to_node`` inject media into a
  graph: a payload of ``{"$media": {...}}`` (or a dict with such a value)
  becomes a real ``MediaItem``. Sources: ``path`` (a file on the server,
  restricted to the allowed roots below), ``base64`` (inline data, with
  optional ``mime``/``filename``) or ``ref`` (a blob already in the store,
  e.g. from another node's ``preview_url``).
- ``load_media_for_tool()`` backs the ``get_media`` tool, which returns an
  image or audio item as real MCP image/audio content - so a model can
  actually look at a frame - optionally shrunk first to save tokens.

``path`` access is limited because the MCP endpoint is unauthenticated by
default (``PSF_MCP_API_KEY`` is opt-in): only files under the files dir,
the data dir and any extra directories in ``PSF_MCP_MEDIA_ROOTS``
(``os.pathsep``-separated) can be read.
"""
from __future__ import annotations

import base64
import binascii
import io
import os
from typing import Any

from ..core.blob_store import get_blob_store
from ..core.media import MediaItem, guess_mime, kind_for_mime
from ..core.media_http import latest_media, max_upload_bytes
from ..core.paths import data_dir, files_dir


def allowed_media_roots() -> list[str]:
    roots = [files_dir(), data_dir()]
    extra = os.environ.get("PSF_MCP_MEDIA_ROOTS", "")
    roots += [r for r in extra.split(os.pathsep) if r.strip()]
    return [os.path.realpath(r) for r in roots]


def _check_path(path: str) -> str:
    real = os.path.realpath(path)
    for root in allowed_media_roots():
        try:
            if os.path.commonpath([real, root]) == root:
                return real
        except ValueError:  # different drives on Windows
            continue
    raise ValueError(
        f"path {path!r} is outside the allowed media directories "
        f"({', '.join(allowed_media_roots())}); add more via PSF_MCP_MEDIA_ROOTS"
    )


def media_from_spec(spec: dict) -> MediaItem:
    if not isinstance(spec, dict):
        raise ValueError("$media must be an object with path, base64 or ref")
    limit = max_upload_bytes()
    mime = spec.get("mime")
    meta = {k: spec[k] for k in ("filename",) if spec.get(k)}
    if spec.get("path"):
        real = _check_path(str(spec["path"]))
        if not os.path.isfile(real):
            raise ValueError(f"no such file: {spec['path']}")
        if os.path.getsize(real) > limit:
            raise ValueError("file larger than PSF_MAX_UPLOAD_MB")
        with open(real, "rb") as f:
            data = f.read()
        meta.setdefault("filename", os.path.basename(real))
        meta["source_path"] = real
    elif spec.get("base64"):
        raw = str(spec["base64"])
        if raw.startswith("data:") and "," in raw:
            header, raw = raw.split(",", 1)
            mime = mime or header[5:].split(";", 1)[0] or None
        try:
            data = base64.b64decode(raw, validate=False)
        except (binascii.Error, ValueError) as e:
            raise ValueError(f"invalid base64: {e}") from None
        if len(data) > limit:
            raise ValueError("data larger than PSF_MAX_UPLOAD_MB")
    elif spec.get("ref"):
        # accept a preview_url too: /media/<ref>?mime=image/png
        ref = str(spec["ref"]).split("?", 1)[0].rsplit("/", 1)[-1]
        store = get_blob_store()
        if not store.exists(ref):
            raise ValueError("unknown or expired media ref")
        head = store.read_head(ref, 64)
        mime = mime or guess_mime(head, meta.get("filename"))
        return MediaItem(kind=spec.get("kind") or kind_for_mime(mime), mime=mime, ref=ref, meta=meta)
    else:
        raise ValueError("$media needs one of: path, base64, ref")
    mime = mime or guess_mime(data, meta.get("filename"))
    return MediaItem.from_bytes(data, kind=spec.get("kind") or None, mime=mime, meta=meta)


def resolve_media_payload(payload: Any) -> Any:
    """Replace ``{"$media": {...}}`` - the payload itself, or any value one
    level down in a dict payload - with a ``MediaItem``."""
    if isinstance(payload, dict) and set(payload) == {"$media"}:
        return media_from_spec(payload["$media"])
    if isinstance(payload, dict):
        return {k: resolve_media_payload(v) if isinstance(v, dict) and set(v) == {"$media"} else v
                for k, v in payload.items()}
    return payload


def _shrink_image(data: bytes, max_side: int) -> tuple[bytes, str] | None:
    """Downscale to ``max_side`` as JPEG (PNG if it has alpha); ``None`` if
    Pillow is missing or nothing needs to change."""
    try:
        from PIL import Image
    except ImportError:
        return None
    img = Image.open(io.BytesIO(data))
    if max(img.size) <= max_side:
        return None
    img.thumbnail((max_side, max_side))
    buf = io.BytesIO()
    if img.mode in ("RGBA", "LA", "P"):
        img.save(buf, format="PNG")
        return buf.getvalue(), "image/png"
    img.convert("RGB").save(buf, format="JPEG", quality=85)
    return buf.getvalue(), "image/jpeg"


def load_media_for_tool(ref: str | None, node_id: str | None, max_side: int | None, nodes: dict) -> dict:
    """Resolve the ``get_media`` tool's arguments to ``{"summary": ...,
    "mime": ..., "data": bytes}``; ``data`` is only set for images and
    audio (the kinds MCP can carry as content)."""
    if node_id:
        node = nodes.get(node_id)
        if node is None:
            raise ValueError("node not found")
        item = latest_media(node)
        if item is None:
            raise ValueError(f"node {node_id!r} has no media item in its recent history")
    elif ref:
        item = media_from_spec({"ref": ref})
    else:
        raise ValueError("pass ref (a media ref or preview_url) or node_id")
    summary = item.summary()
    try:
        summary["ref"] = item.ensure_ref()
    except Exception:
        pass
    major = item.mime.split("/", 1)[0]
    if major not in ("image", "audio"):
        return {"summary": summary, "mime": item.mime, "data": None}
    data = item.get_bytes()
    mime = item.mime
    if major == "image" and max_side:
        shrunk = _shrink_image(data, max_side)
        if shrunk is not None:
            data, mime = shrunk
            summary["returned_as"] = {"mime": mime, "max_side": max_side}
    cap = int(float(os.environ.get("PSF_MCP_MEDIA_MAX_MB", "10")) * 1024 * 1024)
    if len(data) > cap:
        raise ValueError(f"media is {len(data)} bytes, above PSF_MCP_MEDIA_MAX_MB - pass a smaller max_side")
    return {"summary": summary, "mime": mime, "data": data}
