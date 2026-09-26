# Changelog

## Unreleased
- Media foundation (phase 1 of `docs/plans/media_types_plan.md`): `MediaItem` data type with MIME sniffing, disk-backed blob store with TTL/size limit and background cleanup
- Binary-safe node stats and live-view history; `manual_emit()` replays the real payload
- JSON-safe live view, reflection and MCP results for binary items (no more HTTP 500), new `GET /media/{ref}` endpoint with `Range` support
- Media phase 7: MCP `send_to_node` accepts `{"$media": ...}` (file under the files/data dirs, base64 or blob ref), new MCP tool `get_media` (image/audio content); Docker target `runtime-media` (ffmpeg + media extras) selectable with `PSF_IMAGE_TARGET`, `PSF_MEMORY_LIMIT` for the prod compose file, dependencies installed only from pyproject; example workflows `image_thumbnails`, `audio_transcribe`, `video_vision`; user-guide chapter on media; JSONOutputNode serializes media items as summaries
- Editor: thumbnail of the newest image/video frame on the node tile, switchable per node from the context menu (saved as the editor-only config key `_preview`)
- Port data types (phase 6b): advisory `dtype` per port (`GET /port-dtypes`), mismatch warnings from `/nodes/connect`, `/workflows`, the MCP `connect_nodes` tool and `Engine.validate()`; the editor colors ports by type, draws mismatched wires red and shows a legend
- Video nodes (phase 5, PyAV via `pystreamflow[video]` or the ffmpeg executable): Decode (files, uploads, RTSP/HTTP streams with reconnect; fps/size sampling; audio track), FrameSample, Encode (mp4 H.264/AAC, webm VP9/Opus; segments, flush, audio port), Info, Thumbnail
- Fix: in the editor, wires drawn after Import/Load Demo (before Run) were rejected by the backend and removed from the canvas; such nodes are now staged and their wires are applied on Run
- Fix: two edges between the same pair of nodes no longer share one pipe in `Engine._wire_edges()`
- Audio nodes (phase 4, optional `pystreamflow[audio]` extra / numpy + soundfile, ffmpeg for AAC/M4A/Opus and video audio tracks): Decode (whole or chunked), Encode (wav/flac/ogg/mp3/m4a/opus), Resample, Gain, Normalize, Level, Segment (silence/time); SpeechToTextNode with an OpenAI-compatible API backend or local faster-whisper (`pystreamflow[stt]`)
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
