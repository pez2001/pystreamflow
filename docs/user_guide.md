# PyStreamFlow User Guide

## Quick Start
```bash
docker compose up -d
```
* UI: http://localhost:8000/ui
* API: http://localhost:8000
* Web inputs: http://localhost:8080
* MCP: http://localhost:8000/mcp

This also starts a `caddy` reverse-proxy container (see "HTTPS via Caddy"
below) that puts everything above behind a real TLS connection too, at
whatever hostname the Caddyfile names - `https://<that hostname>` for the
UI/API, `https://<that hostname>/mcp` for MCP. Plain HTTP on 8000/8080/9000
still works side by side, so nothing that already points at an `http://`
URL breaks while you switch clients over.

## HTTPS via Caddy

`docker-compose.yml`/`docker-compose.prod.yml` both include a `caddy`
service that terminates HTTPS in front of the `pystreamflow` service, using
the `Caddyfile` in this repo's root. Out of the box it's configured for
`mcp.lan` - edit that one line in the Caddyfile to whatever hostname you
actually use, then `docker compose up -d --build` to pick it up.

**If your hostname isn't a real, publicly-registered domain** (a LAN name
like `mcp.lan`, or an IP address), Caddy can't get a certificate from Let's
Encrypt for it, so it automatically falls back to its own internal CA and
mints a self-signed certificate instead - this needs no configuration, but
every client that connects (a browser, `curl`, LM Studio) will show an
untrusted-certificate warning until that CA is explicitly trusted. This is
a one-time step per client device, not something you redo on every
restart - the `caddy_data` volume is what makes the CA (and therefore
whatever trust you set up) survive a container restart instead of Caddy
silently minting a brand new, differently-untrusted CA each time.

To trust it:
1. Extract the CA's root certificate: `docker compose exec caddy cat
   /data/caddy/pki/authorities/local/root.crt > mcp-lan-ca.crt`
2. Install `mcp-lan-ca.crt` into the trust store of whatever device needs to
   connect without warnings - a browser's own certificate settings, or
   (Windows) `certutil -addstore -f "ROOT" mcp-lan-ca.crt` from an elevated
   prompt, or your OS's equivalent.
3. If a client (LM Studio included) has no way to add a custom trusted CA
   and no explicit "skip certificate verification" option either, plain
   `http://<host>:8000/mcp` alongside the new `https://` URL is still
   there and unaffected - you don't have to migrate every client at once.

**If your hostname *is* a real, publicly-registered domain** (typically
only relevant for `docker-compose.prod.yml`, a genuinely internet-reachable
deployment), point the Caddyfile at that domain instead and Caddy will
automatically obtain and renew a real, publicly-trusted Let's Encrypt
certificate with no other configuration change - no client-side trust step
needed at all in that case.

Once every client you use is on `https://`, you can remove the
`"8000:8000"` line from the compose file's `pystreamflow` service so port
8000 is only reachable through Caddy - not required, just tightens things
up.

## Creating a Workflow
Workflows are YAML files with `nodes` and `edges`.

Example:
```yaml
nodes:
  - id: in1
    type: WebInputNode
    label: Web Input
    config: {}
  - id: up1
    type: TextUpperNode
    label: Upper
    config: {}
  - id: out1
    type: LogOutputNode
    label: Log
    config: {}
edges:
  - source: in1
    target: up1
  - source: up1
    target: out1
```

Load via UI Import or CLI:
```
pystreamflow run workflow.yaml
```

## UI Editor
* Palette left → drag nodes to canvas
* Connect handles source → target
* Expand node for Parameters, Stats, Last items
* Edit parameters inline, changes saved via API
* Global Run/Stop/Pause/Step
* Right-click a node for per-node Start/Stop/Pause/Step/Emit/**Reset** (see [Reset](nodes/index.md#reset))
* Export YAML / Import YAML / Load Demo / **Load Session** (see [Sessions](#sessions) below)
* The first time you open the editor against a daemon that requires a key, a one-time prompt asks for it and remembers it for next time - see [Authentication](#authentication)

## Nodes
* **Inputs**: WebInput, FileInput, JSONInput, MQTTInput, UrlInput, ScriptInput, Timer, ListStrings
* **Outputs**: FileOutput, LogOutput, JSONOutput, WebOutput, MQTTOutput, ScriptedOutput, Display
* **Modifiers**: Grep, Merge, Fork, Template, JSONModify, EncodingConvert, LineSplitter, Tokenizer, LineBuffer
* **Logic**: And, Or, Not, Xor, Nand, Nor, Xnor, Compare
* **Numeric**: Add, Sub, Mul, Div, Mod, Pow, Min, Max, Clamp, Round, Abs
* **Text**: Upper, Lower, Trim, Replace, Substring, Reverse, Title, Strip, Split, Join
* **Trigger**: Trigger, TimerTrigger, TriggerOn/Off/Pause, TriggerIf, TriggerThreshold, TriggerDebounce, TriggerPulse, TriggerToggle
* **Advanced**: Subgraph, Table

## CLI
```
pystreamflow run workflow.yaml          # run headless
pystreamflow daemon                     # start API server
pystreamflow pipe <node_id>             # pipe stdin to node
pystreamflow tail <node_id>             # stream node output
pystreamflow nodes                      # list nodes
pystreamflow validate workflow.yaml     # validate graph
pystreamflow session-create workflow.yaml
pystreamflow session-list
pystreamflow session-start <id>         # (also: session-stop, session-pause, session-resume, session-delete)
```
Most commands above talk to a real running daemon over its HTTP API and need the API key described in [Authentication](#authentication) - pass `--api-key`, or set `$PSF_API_KEY` once so every command picks it up. `pystreamflow --help` and `pystreamflow <command> --help` document every command and flag, including which ones need a key and which (`run` with no piped stdin, `validate`) run entirely locally and don't.

## Web Input / Output
* WebInput nodes expose POST endpoint at `http://localhost:8080/in/{node_id}`
* WebOutput nodes can be read via streaming HTTP
* Multiple WebInput nodes supported via shared server

## Sessions
Sessions allow headless execution with isolation - a session loads one workflow YAML and runs it as its own isolated graph, independent of whatever else is on the shared node registry. A session is fully readable and controllable from every surface, and they all see and act on the exact same sessions on the daemon - there's no separate session state per surface:

* **UI**: click "Load Session" in the editor toolbar to open the Sessions picker - it lists every session with its workflow path, live status, and node/edge counts, with Load into canvas / Start / Pause / Resume / Stop / Delete buttons per row.
* **API**: `POST /sessions` (create), `GET /sessions` (list), `POST /sessions/{id}/start` / `.../pause` / `.../resume` / `.../stop`, `DELETE /sessions/{id}`, `GET /sessions/{id}/workflow`.
* **CLI**: `session-create`, `session-list`, `session-start`, `session-pause`, `session-resume`, `session-stop`, `session-delete` - see the CLI section above. Each is a separate process talking to the daemon over HTTP, so e.g. a session created by `session-create` genuinely persists and is visible to a later `session-list` call, the editor's picker, or an MCP client - not just within the one command that created it.
* **MCP**: `create_session`, `list_sessions`, `start_session`, `pause_session`, `resume_session`, `stop_session`, `delete_session` tools, same shape as the HTTP API.

## Authentication

The main HTTP API - `/nodes`, `/sessions`, `/parse_yaml`, `/connect`, and everything else the editor UI itself calls - requires an API key on every request, **on by default**. If you never set `PSF_API_KEY` yourself, the daemon generates a random one the first time it starts, logs it to the console, and saves it to `data/api_key.txt` (or wherever `$PSF_API_KEY_FILE` points) so you can find it again later without restarting the daemon (a restart reuses the same saved key rather than generating a new one).

Send the key as either header - `Authorization: Bearer <key>` or `X-API-Key: <key>` - both are accepted everywhere.

A few routes are always public, with no key needed, so the editor page itself can load before you've entered anything: the page shell (`/`, `/ui`), its static JS/CSS (`/static/*`), and `/health`/`/version`. A workflow's own `ApiInputNode`/`ApiOutputNode` routes (`/api/*`) are also public by design - those exist specifically so an external system can call *into* your running graph (the same idea as `WebInputNode`'s own standalone server), independent of this admin key.

* **Editor UI**: the first authenticated request that fails pops up a one-time "API Key Required" prompt automatically; paste the key in and it's remembered in your browser (`localStorage`) for next time, attached to every request from then on.
* **CLI**: pass `--api-key` on any command that needs it, or set `$PSF_API_KEY` once in your shell so every command picks it up with no flag needed.
* **MCP**: unaffected - `/mcp/*` keeps its own, separate, opt-in key (`$PSF_MCP_API_KEY`), unset by default. The two keys are independent; setting one doesn't require or affect the other.

## Working with Images, Audio and Video
Media travels through a workflow as **media items**: the file's bytes plus its type and metadata (file name, size, image dimensions, duration, timestamps). Text nodes see a short description such as `<image/png 1920x1080 3.1MB>`, and the editor's live view shows a real preview (image, audio or video player). Large payloads are kept in a blob store under `data/blobs` (expired after 10 idle minutes, capped at 1 GB - see `PSF_BLOB_*` in the developer guide).

**Install the media support.** The core install has no media libraries; those node types appear greyed out in the palette with an install hint.

* Docker: `PSF_IMAGE_TARGET=runtime-media docker compose up -d --build` builds the `runtime-media` image (ffmpeg, Pillow, numpy/soundfile, PyAV). For local speech recognition add `PSF_MEDIA_EXTRAS=media,stt` (large). With `docker-compose.prod.yml`, also raise `PSF_MEMORY_LIMIT` (default 512M) - e.g. `2G` - for video work.
* pip: `pip install 'pystreamflow[media]'`, or only `[image]`, `[audio]`, `[video]`; `[stt]` for faster-whisper. AAC/M4A audio and the video nodes' fallback need the `ffmpeg` executable.

**Getting media into a workflow**

* A folder: DirectoryInputNode with `emit_as: media` (and `media_only: true`).
* One file: MediaFileInputNode (re-emits when the file changes).
* An upload: WebInputNode/ApiInputNode accept `curl -F file=@photo.jpg http://localhost:8080/api/<uri>` or a raw body with `Content-Type: image/png` (limit `max_upload_mb`, default 100).
* A camera or stream: VideoDecodeNode with `source: rtsp://...`.
* From an AI assistant: the MCP tool `send_to_node` with `{"$media": {"path": "files/photo.jpg"}}` or base64 data.

**Getting media out**: MediaFileOutputNode (one file per item), WebOutputNode/ApiOutputNode (`…/media` serves the newest item with its real content type), Base64EncodeNode with `data_url: true`, or the MCP tool `get_media`.

**Examples** in `workflows/` (Import them in the editor):

* `image_thumbnails.yaml` - images dropped into `files/photos` become 800 px WebP thumbnails in `files/thumbnails` (phone photos rotated upright).
* `audio_transcribe.yaml` - upload a recording to `/api/transcribe`; it is cut at pauses, resampled to 16 kHz and transcribed, the text goes to `files/transcripts.txt` and MQTT. Needs a speech-to-text server (OpenAI-compatible) or `backend: local`.
* `video_vision.yaml` - `files/videos/input.mp4` is sampled at one frame per second and described by a vision model in LM Studio; one JSON line per frame in `files/vision_log.jsonl`.

Nodes whose newest item is an image or video frame show a small thumbnail at the bottom of their tile; right-click a node → "Hide preview on node" to turn it off for that node (saved with the workflow).

All media node types and their options are listed under "Media Nodes", "Image Nodes", "Audio Nodes" and "Video Nodes" in the [node reference](nodes/index.md). Ports are coloured by the kind of data they carry; a red wire means the two ends probably don't fit (for example an image into a text node).

## Tips
* Nodes auto-start on first input if `auto_start: true`
* Use Trigger nodes to control start/stop/pause/resume/step/emit/reset of other nodes - and a node's `control` input accepts more than one Trigger source at once, so e.g. one node can pause a target while a separate node independently resumes it
* Use `reset` (right-click a node → "♻ Reset", or a Trigger node's `action: reset`) to clear a node's counters/contents without stopping it - see [Reset](nodes/index.md#reset)
* Use SubgraphNode to encapsulate reusable parts
* Stats are live in UI and via `/nodes/{id}/stats`
