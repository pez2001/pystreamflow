"""Disk-backed, content-addressed store for large media payloads.

Phase 1 of the media plan (``docs/plans/media_types_plan.md``, 1.2): a
``MediaItem`` (see ``core/media.py``) whose payload is larger than the
inline threshold doesn't carry its bytes through the graph at all - it
carries a *ref*, the SHA-256 of the payload, and the bytes live here, in
``<PSF_DATA_DIR>/blobs/<sha256>``. That keeps every per-item cost that
scales with the number of places an item is held - fan-out to several
consumers, ``BaseNode._last_items``' history, the live view - down to the
size of a short string, instead of one full copy of a video frame each.

Content addressing gives de-duplication for free: fanning the same item
out to five consumers, or a source emitting the same image repeatedly,
still stores exactly one file.

Lifetime is TTL-based rather than reference-counted: an item can sit in
a pipe, in any number of ``_last_items`` buffers, or in a client's
browser tab as a preview URL, none of which this store could reliably
track. Each blob instead expires ``ttl_s`` seconds after it was last
written or read (its file mtime is refreshed on both), and a hard
``max_bytes`` cap evicts the least-recently-used blobs first so a
long-running video workflow can never fill the disk. ``cleanup()`` does
both; ``ensure_cleanup_task()`` runs it periodically in the background.

Configuration (environment variables, read once when the default store
is first built):

- ``PSF_BLOB_DIR``: store location (default ``<PSF_DATA_DIR>/blobs``)
- ``PSF_BLOB_TTL_S``: idle lifetime per blob in seconds (default 600)
- ``PSF_BLOB_MAX_MB``: total size cap in MB (default 1024)
- ``PSF_BLOB_CLEANUP_INTERVAL_S``: background cleanup period (default 60)
"""
import asyncio
import hashlib
import logging
import os
import re
import tempfile
import threading
import time

from .paths import data_dir

logger = logging.getLogger("pystreamflow.blob_store")

_REF_RE = re.compile(r"^[0-9a-f]{64}$")


def is_valid_ref(ref: str) -> bool:
    """A ref is exactly a lowercase hex SHA-256 - checked before every
    filesystem access so a ref taken from a URL (``GET /media/{ref}``)
    can never be used for path traversal."""
    return isinstance(ref, str) and bool(_REF_RE.match(ref))


class BlobStore:
    def __init__(self, root: str, ttl_s: float = 600.0, max_bytes: int = 1024 * 1024 * 1024):
        self.root = root
        self.ttl_s = float(ttl_s)
        self.max_bytes = int(max_bytes)
        self._lock = threading.Lock()
        self.evicted = 0
        os.makedirs(self.root, exist_ok=True)
        self._total = self._scan_total()

    def _scan_total(self) -> int:
        total = 0
        for entry in self._entries():
            total += entry[2]
        return total

    def _entries(self) -> list[tuple[str, float, int]]:
        """``(ref, mtime, size)`` for every blob currently on disk."""
        out = []
        try:
            names = os.listdir(self.root)
        except FileNotFoundError:
            return out
        for name in names:
            if not is_valid_ref(name):
                continue
            try:
                st = os.stat(os.path.join(self.root, name))
            except FileNotFoundError:
                continue
            out.append((name, st.st_mtime, st.st_size))
        return out

    def path(self, ref: str) -> str:
        if not is_valid_ref(ref):
            raise KeyError(ref)
        return os.path.join(self.root, ref)

    def exists(self, ref: str) -> bool:
        return is_valid_ref(ref) and os.path.isfile(os.path.join(self.root, ref))

    def put(self, data: bytes) -> str:
        """Store ``data`` and return its ref. Idempotent: storing the same
        bytes twice writes once and just refreshes the blob's TTL."""
        data = bytes(data)
        ref = hashlib.sha256(data).hexdigest()
        target = os.path.join(self.root, ref)
        with self._lock:
            if os.path.isfile(target):
                self._touch(target)
                return ref
            os.makedirs(self.root, exist_ok=True)
            # Write to a temp file in the same directory, then rename, so a
            # concurrent reader never sees a half-written blob.
            fd, tmp = tempfile.mkstemp(dir=self.root, prefix=".tmp-")
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                os.replace(tmp, target)
            except BaseException:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
                raise
            self._total += len(data)
            over = self._total > self.max_bytes
        if over:
            self._enforce_limit(keep=ref)
        return ref

    def get(self, ref: str) -> bytes:
        """Return a blob's bytes, refreshing its TTL. Raises ``KeyError``
        if it doesn't exist (never stored, or already expired)."""
        p = self.path(ref)
        try:
            with open(p, "rb") as f:
                data = f.read()
        except FileNotFoundError:
            raise KeyError(ref) from None
        self._touch(p)
        return data

    def size(self, ref: str) -> int:
        try:
            return os.path.getsize(self.path(ref))
        except FileNotFoundError:
            raise KeyError(ref) from None

    def read_head(self, ref: str, n: int = 64) -> bytes:
        try:
            with open(self.path(ref), "rb") as f:
                return f.read(n)
        except FileNotFoundError:
            raise KeyError(ref) from None

    def delete(self, ref: str) -> bool:
        with self._lock:
            return self._delete_locked(ref)

    def _delete_locked(self, ref: str) -> bool:
        try:
            p = self.path(ref)
            n = os.path.getsize(p)
            os.unlink(p)
        except (FileNotFoundError, KeyError):
            return False
        self._total = max(0, self._total - n)
        return True

    @staticmethod
    def _touch(p: str) -> None:
        try:
            os.utime(p, None)
        except OSError:
            pass

    def _enforce_limit(self, keep: str | None = None) -> int:
        """Evict least-recently-used blobs until the store fits within
        ``max_bytes``. ``keep`` (the blob just written) is never evicted
        by the put() that triggered this."""
        removed = 0
        with self._lock:
            entries = sorted(self._entries(), key=lambda e: e[1])
            self._total = sum(e[2] for e in entries)
            for ref, _mtime, _size in entries:
                if self._total <= self.max_bytes:
                    break
                if ref == keep:
                    continue
                if self._delete_locked(ref):
                    removed += 1
            self.evicted += removed
        if removed:
            logger.info("blob store over %d bytes - evicted %d blob(s)", self.max_bytes, removed)
        return removed

    def cleanup(self, now: float | None = None) -> int:
        """Remove expired blobs (idle longer than ``ttl_s``) and stale temp
        files, then enforce ``max_bytes``. Returns how many were removed."""
        now = time.time() if now is None else now
        removed = 0
        with self._lock:
            for ref, mtime, _size in self._entries():
                if now - mtime > self.ttl_s and self._delete_locked(ref):
                    removed += 1
            try:
                for name in os.listdir(self.root):
                    if name.startswith(".tmp-"):
                        p = os.path.join(self.root, name)
                        try:
                            if now - os.path.getmtime(p) > self.ttl_s:
                                os.unlink(p)
                        except OSError:
                            pass
            except FileNotFoundError:
                pass
            self.evicted += removed
        return removed + self._enforce_limit()

    def stats(self) -> dict:
        entries = self._entries()
        return {
            "count": len(entries),
            "bytes": sum(e[2] for e in entries),
            "max_bytes": self.max_bytes,
            "ttl_s": self.ttl_s,
            "evicted": self.evicted,
        }


_default_store: BlobStore | None = None
_default_lock = threading.Lock()


def get_blob_store() -> BlobStore:
    """The process-wide store, built from the environment on first use."""
    global _default_store
    with _default_lock:
        if _default_store is None:
            root = os.environ.get("PSF_BLOB_DIR") or os.path.join(data_dir(), "blobs")
            ttl = float(os.environ.get("PSF_BLOB_TTL_S", "600"))
            max_mb = float(os.environ.get("PSF_BLOB_MAX_MB", "1024"))
            _default_store = BlobStore(root, ttl_s=ttl, max_bytes=int(max_mb * 1024 * 1024))
        return _default_store


def set_blob_store(store: BlobStore | None) -> None:
    """Replace the process-wide store (``None`` resets it to be rebuilt
    from the environment on next use). Mainly for tests."""
    global _default_store
    with _default_lock:
        _default_store = store


_cleanup_task: asyncio.Task | None = None


async def _cleanup_loop(interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await asyncio.to_thread(get_blob_store().cleanup)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("blob store cleanup failed")


def ensure_cleanup_task() -> asyncio.Task:
    """Start the periodic background cleanup on the running event loop,
    unless one is already running there. Safe to call from every
    ``Engine.run()`` and from the API server's lifespan - they all share
    this single task."""
    global _cleanup_task
    loop = asyncio.get_running_loop()
    task = _cleanup_task
    if task is None or task.done() or task.get_loop() is not loop:
        interval = float(os.environ.get("PSF_BLOB_CLEANUP_INTERVAL_S", "60"))
        _cleanup_task = asyncio.create_task(_cleanup_loop(interval))
    return _cleanup_task


async def stop_cleanup_task() -> None:
    global _cleanup_task
    task, _cleanup_task = _cleanup_task, None
    if task is not None and not task.done() and task.get_loop() is asyncio.get_running_loop():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
