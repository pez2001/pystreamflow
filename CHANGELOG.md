# Changelog

## Unreleased
- Media foundation (phase 1 of `docs/plans/media_types_plan.md`): `MediaItem` data type with MIME sniffing, disk-backed blob store with TTL/size limit and background cleanup
- Binary-safe node stats and live-view history; `manual_emit()` replays the real payload
- JSON-safe live view, reflection and MCP results for binary items (no more HTTP 500), new `GET /media/{ref}` endpoint with `Range` support
- Image nodes (phase 3, optional `pystreamflow[image]` extra / Pillow): Decode, Resize, Crop, Rotate, Flip, Convert, Filter, Info, Thumbnail on a shared `MediaTransformNode` base; LMStudioNode sends images to vision models (new `prompt` input, `image_prompt` config); `GET /node-availability` and a greyed-out palette for node types whose dependency is missing
- Media file I/O (phase 2): new `MediaFileInputNode` and `MediaFileOutputNode`; `DirectoryInputNode` gains `extensions`/`media_only`/`emit_as: media`; WebInputNode/ApiInputNode accept file uploads (multipart, raw media bodies, urlencoded forms) with a size limit; WebOutputNode/ApiOutputNode serve the newest media item at `…/media`; Base64 nodes handle media items and data URLs; new dependency `python-multipart`
- Editor live view (phase 6a): image/audio/video previews of the newest media item, "last frame" mode with pause for frame streams, media gallery in the live-view modal
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
