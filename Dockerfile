# Multi-stage build for production
FROM python:3.11-slim AS builder

WORKDIR /build
COPY pyproject.toml README.md ./
COPY pystreamflow ./pystreamflow
RUN pip install --upgrade pip && pip wheel --no-cache-dir --wheel-dir /wheels .

FROM python:3.11-slim AS runtime

WORKDIR /app
RUN useradd -m appuser
COPY --from=builder /wheels /wheels
RUN pip install --no-index --find-links /wheels pystreamflow && pip install fastapi uvicorn pyyaml pydantic httpx prometheus-client typer
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
# the same three directories the compose files rely on.
RUN mkdir -p /app/logs /app/files /app/data \
    && chown -R appuser:appuser /app && chmod -R 755 /app

USER appuser
EXPOSE 8000 8080 9000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"
CMD ["uvicorn", "pystreamflow.api.server:app", "--host", "0.0.0.0", "--port", "8000"]
