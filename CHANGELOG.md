# Changelog

## Unreleased
- Media foundation (phase 1 of `docs/plans/media_types_plan.md`): `MediaItem` data type with MIME sniffing, disk-backed blob store with TTL/size limit and background cleanup
- Binary-safe node stats and live-view history; `manual_emit()` replays the real payload
- JSON-safe live view, reflection and MCP results for binary items (no more HTTP 500), new `GET /media/{ref}` endpoint with `Range` support
- Per-edge `buffer: {maxsize, drop_policy}`, new `drop_oldest` policy, `drop` now drops immediately on a full queue, bounded defaults for media output ports, drop and blob-store metrics

## v1.0.0 - 2026-09-08
- Production release
- Core hardening: backpressure, health checks, versioning, retries
- Multi WebInput shared server
- LM Studio node
- Live view API
- MCP server
- Multi-stage Dockerfile + docker-compose.prod.yml
- Prometheus metrics & structured logging
- CI pipeline, tests, docs, tutorials
