"""Minimal ffmpeg wrapper for media conversion (media plan phases 4/5).

Used as the fallback for formats the pure-Python decoders can't read or
write (for audio: AAC/M4A, audio tracks inside MP4/WebM, ...). Always
file-to-file through a temporary directory rather than stdin/stdout pipes:
MP4-family inputs need a seekable file (their index may sit at the end),
and a WAV written to a pipe has no valid length in its header.

Blocking by design - callers run it in a worker thread
(``asyncio.to_thread``), like every other decode/encode step. Uses plain
``subprocess.run`` with a timeout, which also works on Windows (unlike
``core/subprocess_exec.py``'s process-group handling; see the plan's
risk note).

``PSF_FFMPEG`` overrides the executable; otherwise ``ffmpeg`` is looked up
on ``PATH``.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile


class FFmpegError(RuntimeError):
    pass


class FFmpegNotFound(FFmpegError):
    pass


def ffmpeg_path() -> str | None:
    configured = os.environ.get("PSF_FFMPEG")
    if configured:
        return configured if (os.path.isfile(configured) or shutil.which(configured)) else None
    return shutil.which("ffmpeg")


def convert(data: bytes, output_ext: str, output_args: list[str] | None = None,
            input_ext: str = "bin", timeout: float = 300.0) -> bytes:
    """Convert ``data`` with ffmpeg to a file with extension ``output_ext``
    (which selects the container) and return its bytes."""
    exe = ffmpeg_path()
    if not exe:
        raise FFmpegNotFound("ffmpeg not found - install it or set PSF_FFMPEG")
    with tempfile.TemporaryDirectory(prefix="psf-ffmpeg-") as tmp:
        src = os.path.join(tmp, f"in.{input_ext.lstrip('.') or 'bin'}")
        dst = os.path.join(tmp, f"out.{output_ext.lstrip('.')}")
        with open(src, "wb") as f:
            f.write(data)
        cmd = [exe, "-hide_banner", "-loglevel", "error", "-nostdin", "-y", "-i", src,
               *(output_args or []), dst]
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise FFmpegError(f"ffmpeg timed out after {timeout:g}s") from None
        if proc.returncode != 0 or not os.path.exists(dst):
            msg = proc.stderr.decode("utf-8", "replace").strip().splitlines()
            raise FFmpegError(f"ffmpeg failed: {msg[-1] if msg else 'exit code %d' % proc.returncode}")
        with open(dst, "rb") as f:
            return f.read()
