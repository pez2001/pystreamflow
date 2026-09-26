# Multi-stage build for production.
#
# Two runtime images from one Dockerfile (media plan phase 7):
#
#   runtime        (default target) - the core install, no media extras.
#                  Image/audio/video nodes show up greyed out in the editor.
#   runtime-media  - adds the ffmpeg executable and the `media` extras
#                  (Pillow, numpy + soundfile, PyAV), so every media node
#                  works. Build it with `docker build --target runtime-media .`
#                  or set PSF_IMAGE_TARGET=runtime-media for docker compose.
#
# PSF_MEDIA_EXTRAS picks the extras for runtime-media, e.g. "media,stt" to
# also get faster-whisper for SpeechToTextNode's local backend (large).

ARG PSF_MEDIA_EXTRAS=media

FROM python:3.11-slim AS builder
ARG PSF_MEDIA_EXTRAS

WORKDIR /build
COPY pyproject.toml README.md ./
COPY pystreamflow ./pystreamflow
# One wheelhouse for both targets: pystreamflow, all of its core
# dependencies, and the optional media extras. `runtime` installs only
# the core from it; `runtime-media` adds the extras.
RUN pip install --upgrade pip \
    && pip wheel --no-cache-dir --wheel-dir /wheels ".[${PSF_MEDIA_EXTRAS}]"

FROM python:3.11-slim AS base

WORKDIR /app
RUN useradd -m appuser
COPY --from=builder /wheels /wheels
# Every dependency comes from pyproject.toml via the wheelhouse. (This used
# to be followed by a hand-written `pip install fastapi uvicorn pyyaml ...`
# that duplicated part of pyproject's list - and missed mcp and paho-mqtt.)
RUN pip install --no-cache-dir --no-index --find-links /wheels pystreamflow
COPY pystreamflow ./pystreamflow
COPY workflows ./workflows
COPY workflows/example_workflow.yaml ./example_workflow.yaml
# Pre-create the mount points docker-compose.yml/docker-compose.prod.yml
# bind-mount ./logs, ./files and ./data onto (/app/logs, /app/files,
# /app/data - see PSF_LOGS_DIR/PSF_FILES_DIR/PSF_DATA_DIR, which
# FileOutputNode/LogOutputNode/ProcessInputNode/ShellInputNode/
# ProcessOutputNode all default into). This matters even though a bind
# mount replaces whatever's here at runtime: it means `docker run`
# without any volumes at all (no compose file) still has real,
# already-owned-by-appuser directories to write into instead of hitting
# a missing-directory error on the very first item, and it documents
# the same three directories the compose files rely on. The media blob
# store lives in /app/data/blobs.
RUN mkdir -p /app/logs /app/files /app/data \
    && chown -R appuser:appuser /app && chmod -R 755 /app

EXPOSE 8000 8080 9000
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "pystreamflow.api.server:app", "--host", "0.0.0.0", "--port", "8000"]

FROM base AS runtime-media
ARG PSF_MEDIA_EXTRAS
USER root
# ffmpeg: fallback decoder/encoder for AAC/M4A, audio tracks of videos and
# the video nodes' ffmpeg backend (see pystreamflow/core/ffmpeg.py).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir --no-index --find-links /wheels "pystreamflow[${PSF_MEDIA_EXTRAS}]"
USER appuser

# Last stage = default target, so a plain `docker build .` stays the slim image.
FROM base AS runtime
USER appuser
