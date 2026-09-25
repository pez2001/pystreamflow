"""HTTP plumbing for media on the nodes' shared web server (media plan
phase 2): turning an upload into ``MediaItem``s, and serving a
``MediaItem`` back with its real Content-Type.

Used by ``core/web_server.py``'s ``register_input()`` (so ``WebInputNode``
and ``ApiInputNode`` accept file uploads besides JSON) and by
``WebOutputNode``/``ApiOutputNode`` (which serve the latest media item
they received).

Accepted request bodies:

- ``multipart/form-data`` (a browser ``<form>`` upload, ``curl -F
  file=@photo.jpg``): every file part becomes one ``MediaItem``; plain
  text fields are attached to each item as ``meta["form"]``. A form with
  no file parts at all is treated like a JSON object of its fields.
- ``application/octet-stream``, ``image/*``, ``audio/*``, ``video/*``
  (``curl --data-binary @clip.mp4 -H 'Content-Type: video/mp4'``): the
  raw body becomes one ``MediaItem``. For ``application/octet-stream``
  the type is sniffed from the bytes, then from a ``filename`` query
  parameter or ``X-Filename`` header if given.
- ``application/x-www-form-urlencoded`` (a plain HTML form): emitted as a
  JSON object of its fields.
- anything else: parsed as JSON, exactly as before this module existed.
"""
from __future__ import annotations

import asyncio
import json
import os
import re

from fastapi import Request
from fastapi.responses import FileResponse, JSONResponse

from .blob_store import get_blob_store
from .media import MediaItem, guess_mime

_RAW_MEDIA_TYPES = ("image/", "audio/", "video/")
_FILENAME_RE = re.compile(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', re.IGNORECASE)


class UploadTooLarge(Exception):
    pass


class BadRequest(Exception):
    pass


class InvalidJSON(BadRequest):
    pass


def max_upload_bytes() -> int:
    """Default per-request/per-file upload limit: ``PSF_MAX_UPLOAD_MB``
    (default 100). A node can override it with its own ``max_upload_mb``
    config."""
    return int(float(os.environ.get("PSF_MAX_UPLOAD_MB", "100")) * 1024 * 1024)


def node_upload_limit(node) -> int:
    value = (getattr(node, "config", None) or {}).get("max_upload_mb")
    if value in (None, ""):
        return max_upload_bytes()
    return int(float(value) * 1024 * 1024)


def _content_type(request: Request) -> str:
    return request.headers.get("content-type", "").split(";", 1)[0].strip().lower()


def is_raw_media_request(request: Request) -> bool:
    ctype = _content_type(request)
    return ctype == "application/octet-stream" or ctype.startswith(_RAW_MEDIA_TYPES)


async def _read_body_limited(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise UploadTooLarge()
    chunks = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > limit:
            raise UploadTooLarge()
        chunks.append(chunk)
    return b"".join(chunks)


def _filename_from_headers(request: Request) -> str | None:
    name = request.query_params.get("filename") or request.headers.get("x-filename")
    if name:
        return os.path.basename(name)
    disposition = request.headers.get("content-disposition", "")
    m = _FILENAME_RE.search(disposition)
    return os.path.basename(m.group(1)) if m else None


async def parse_request(request: Request, limit: int) -> tuple[str, object]:
    """Read one request body. Returns ``("media", [MediaItem, ...])`` for
    uploads, or ``("json", value)`` for everything else. Raises
    ``UploadTooLarge`` or ``BadRequest``."""
    ctype = _content_type(request)
    if ctype == "multipart/form-data":
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > limit:
            raise UploadTooLarge()
        items: list[MediaItem] = []
        fields: dict[str, object] = {}
        async with request.form(max_part_size=min(limit, 16 * 1024 * 1024)) as form:
            files = []
            for key, value in form.multi_items():
                if isinstance(value, str):
                    fields[key] = value
                else:
                    files.append((key, value))
            for key, upload in files:
                if upload.size is not None and upload.size > limit:
                    raise UploadTooLarge()
                data = await upload.read()
                if len(data) > limit:
                    raise UploadTooLarge()
                filename = os.path.basename(upload.filename or "") or None
                declared_type = (upload.content_type or "").split(";", 1)[0].strip().lower()
                mime = guess_mime(data, filename)
                if mime == "application/octet-stream" and declared_type and declared_type != mime:
                    mime = declared_type
                meta = {"field": key}
                if filename:
                    meta["filename"] = filename
                items.append((data, mime, meta))
        if not items:
            return "json", fields
        built = []
        for data, mime, meta in items:
            if fields:
                meta["form"] = dict(fields)
            built.append(await MediaItem.afrom_bytes(data, mime=mime, meta=meta))
        return "media", built

    if ctype == "application/x-www-form-urlencoded":
        declared = request.headers.get("content-length")
        if declared and declared.isdigit() and int(declared) > limit:
            raise UploadTooLarge()
        async with request.form() as form:
            return "json", {k: v for k, v in form.multi_items() if isinstance(v, str)}

    if ctype == "application/octet-stream" or ctype.startswith(_RAW_MEDIA_TYPES):
        data = await _read_body_limited(request, limit)
        if not data:
            raise BadRequest("empty body")
        filename = _filename_from_headers(request)
        if ctype == "application/octet-stream":
            mime = guess_mime(data, filename)
        else:
            mime = ctype
        meta = {"filename": filename} if filename else {}
        return "media", [await MediaItem.afrom_bytes(data, mime=mime, meta=meta)]

    body = await _read_body_limited(request, limit)
    try:
        value = json.loads(body) if body else None
    except ValueError as e:
        raise InvalidJSON(f"invalid JSON body: {e}") from None
    return "json", value


def error_response(exc: Exception) -> JSONResponse:
    if isinstance(exc, UploadTooLarge):
        return JSONResponse(status_code=413, content={"error": "upload too large"})
    if isinstance(exc, InvalidJSON):
        # 422, as FastAPI's own body validation answered before uploads
        # were supported.
        return JSONResponse(status_code=422, content={"error": str(exc)})
    return JSONResponse(status_code=400, content={"error": str(exc)})


async def media_response(item: MediaItem, download_name: str | None = None):
    """Serve ``item`` with its own Content-Type. Always goes through a
    file in the blob store, so ``FileResponse`` answers ``Range`` requests
    (needed for seeking in ``<audio>``/``<video>``) for inline items too.
    Returns 410 if the blob has already expired."""
    try:
        ref = await asyncio.to_thread(item.ensure_ref)
    except Exception:
        ref = None
    store = get_blob_store()
    if not ref or not store.exists(ref):
        return JSONResponse(status_code=410, content={"error": "media expired"})
    headers = {"X-Content-Type-Options": "nosniff", "Cache-Control": "no-cache"}
    name = download_name or item.meta.get("filename")
    if name:
        headers["Content-Disposition"] = f'inline; filename="{os.path.basename(str(name))}"'
    return FileResponse(store.path(ref), media_type=item.mime or "application/octet-stream", headers=headers)


def no_media_response() -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "no media item received yet"})



def latest_media(node) -> MediaItem | None:
    """The newest ``MediaItem`` in a node's live-view history."""
    for entry in reversed(node.get_last()):
        item = entry.get('item') if isinstance(entry, dict) else entry
        if isinstance(item, MediaItem):
            return item
    return None


def latest_item(node):
    """The newest item in a node's history (unwrapped from its entry)."""
    last = node.get_last(1)
    if not last:
        return None
    return last[-1].get('item', last[-1]) if isinstance(last[-1], dict) else last[-1]


def register_media_route(node, path: str, name: str) -> str:
    """``GET <path>`` on the shared web server serves the newest
    ``MediaItem`` the node has passed on, with its own Content-Type and
    ``Range`` support; 404 until there is one."""
    from .web_server import register_route

    async def handler():
        item = latest_media(node)
        if item is None:
            return no_media_response()
        return await media_response(item)

    register_route("get", path, name, handler)
    return path


def json_payload(item) -> str:
    """``json.dumps`` for an SSE/JSON output, with binary payloads and
    ``MediaItem``s turned into JSON-safe summaries first."""
    from .media import to_jsonable

    try:
        return json.dumps(to_jsonable(item))
    except (TypeError, ValueError):
        return json.dumps({'data': str(item)})
