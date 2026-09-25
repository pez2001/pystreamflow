# PyStreamFlow Node Documentation

This page provides online context help for all nodes. Hover over nodes in the editor to see a quick tip with a link to the detailed section below.

## Data vs. raw wires

Every node's real output ports can be wired two different ways, rather than each output needing its own separate "raw" port. An ordinary `data` edge (what you get from a plain drag-and-drop in the editor) delivers the item exactly as the node emits it. A `raw` edge delivers that same item run through the same unwrap step an `attribute` edge already uses, so a `{'value': 'hi', 'index': 3}`-shaped item arrives at the raw wire's target as the bare `'hi'`. Nothing about the source node changes between the two - it's the wire you draw, not the port, that decides which packaging the downstream end receives.

In the node editor: drag normally for a `data` wire. To send a `raw` wire instead, right-click the real output slot first and choose "Mark next wire from here as raw" - the very next wire drawn from that exact slot is sent as `raw` (the marking clears itself after that one wire, whether or not it actually ends up being `raw` - dropping onto an attribute or control input, or dragging from a Trigger-family node, always wins outright over the marking). `data` and `raw` wires are drawn in distinct colors so you can tell them apart on the canvas at a glance.

Two node types manage their output ports differently and aren't part of this shared mechanism: **ForkNode** creates its own numbered `outN`/`rawN` port pair per wire you draw from it, so you can see and manage each fan-out destination as its own port; and **ApiOutputNode** has its own permanent, hand-declared `out`/`raw` port pair, where `raw` is the graph-level counterpart to its `/api/<uri>/raw` HTTP route rather than an instance of the generic raw-edge mechanism above - both of its ports currently carry identical, non-unwrapped items.

## Multiple wires into one port

Every port on a node - a real output, a regular data input, or the reserved `control` input - can have more than one wire attached to it at once, on both ends:

- **Outputs** already fan out: connect one output to as many downstream inputs as you like and every one of them receives every item.
- **`control` inputs** fan in: two or more Trigger-family nodes can each independently drive the same target, and every source's commands are delivered (see the [Trigger System](#trigger-system) section below).
- **Regular data inputs** also fan in: wiring a second edge onto a named input that already has one no longer steals the connection away from the first source. Both sources deliver into that same input, interleaved in whatever order items actually arrive - so `pipe.get()` on that port can return an item from either source on any given call. (Earlier versions of PyStreamFlow silently let a second data edge onto the same input replace the first, with no error - if an old workflow's second wire into an input seemed to have no effect, that's why; it now just works.) There's no ordering or priority between sources - if you need one specific source's items kept separate from another's rather than interleaved, wire each into its own input name (or use **MergeNode**, which is built for combining several sources visually on the canvas under a single node).

Disconnecting one of several edges into the same input removes only that one source; the others keep delivering undisturbed.

### Adding more edge points to a node

A handful of node types have a genuinely variable number of ports rather than a fixed set: **MergeNode**, **AndNode**, **OrNode**, **NandNode**, **NorNode**, **XorNode**, **XnorNode** (variable inputs), **ForkNode**, **SplitByValueNode**, **RoundRobinNode**, **SubgraphNode** (variable outputs), and **SyncBarrierNode**/**LedActivityNode** (a matched inN/outN pair that always grows or shrinks together). These node types show extra buttons right on the node body - "+ input"/"− input", "+ output"/"− output", "+ port"/"− port", or (for SplitByValueNode) "+ value / output"/"− value / output" - that add or remove one numbered port at a time, from 1 up to 8. Use these when you need more genuine edge points on one of these node types than it starts with (MergeNode/ForkNode/etc. start with 4); every other node type's port count is fixed by what it actually does with each named port, so it doesn't get these buttons.

If a node you expect to have these buttons (or the right port names at all) doesn't, and this is the first time you've opened the editor against this server or you're in a fresh/cleared browser profile: reload the page after entering your API key in the "API Key Required" prompt. Versions before this fix didn't automatically recover from the very first schema request failing while no key was set yet, so every node fell back to a single generic input/output until the page was reloaded - current versions retry that request the moment you save a key, so a reload shouldn't be necessary any more, but it's a safe fallback if something still looks off.

**WebInputNode/ApiInputNode/WebOutputNode/WebOutputJSONNode/ApiOutputNode** all share one HTTP server (`core/web_server.py`) and its `host`/`port` config. Bug fix: `host` used to default to `127.0.0.1`, which - unlike on a bare venv/local install - is unreachable from outside a Docker container through that container's own published port (Docker's port-forwarding reaches a container's real network interface, never a loopback-only bind inside it), so these nodes silently didn't work at all for anyone running the packaged `docker-compose.yml` and never overriding `host` themselves (nothing in the editor UI even surfaces this field to prompt you to). Now defaults to `0.0.0.0`; set `host` explicitly back to `127.0.0.1` in a node's config if you specifically want same-container-only reachability. Separately, if you're trying a real HTTP request against one of these nodes rather than injecting via `send_to_node`, the MCP `get_node_config`/`reflect_node` tools (and the equivalent `/nodes/{id}/config`/`/reflection/nodes/{id}` REST endpoints) now include an `http_endpoint` field with the node's real, effective path (e.g. `/api/demo/in`, not just the raw `uri: "demo/in"` config value) and port - config alone never told you the `/api/` prefix ApiInputNode/ApiOutputNode add.

## Input Nodes
- **WebInputNode** – Receives HTTP POST payloads via the shared web input server. Config: `host`, `port`, `path`. [Details](#webinputnode)
- **FileInputNode** – Polls a file for new bytes and emits them. Config: `path`, `poll_interval`.
- **DirectoryInputNode** – Watches a directory and emits newly-seen entries on two separate ports: files on `files`, subdirectories on `dirs`. Config: `path`, `recursive` (bool, default `false` – walk nested subdirectories too), `poll_interval`, `extensions` (optional filter, e.g. `"jpg, png"`), `media_only` (bool – only image/audio/video files), `emit_as` (`path`, the default, or `media` – emit each file as a media item, see [Media Nodes](#media-nodes)). Both `path` and `recursive` can be updated live via an attribute wire.
- **JSONInputNode** – Reads newline-delimited JSON from a real source and emits each parsed value. Config: `source` (`'stdin'`, the default - this process's real standard input - or a file path to tail for newly-appended lines).
- **LMStudioNode** – Calls LM Studio OpenAI-compatible completions. Config: `model`, `base_url`, `api_key`, `system`, `timeout` (seconds to wait for a response once the request is sent - the connection itself still fails fast at a fixed 10s if the server isn't reachable at all; default `600` (10 minutes), raise it further for a large model on slow hardware, since a plain HTTP request has no way to distinguish "still generating" from "hung" other than time). Emits to six distinct ports instead of one mixed shape: `out` (everything below, in one item), `prompt` (echoes what was sent, fires whether or not the request succeeds), `results` (just the final answer text), `reasoning` (a reasoning-capable model's chain-of-thought, when the response actually has one - read from `message.reasoning_content`/`reasoning`, or split out of a `<think>...</think>` block in the content for servers that inline it there instead), `errors` (just the error string, only on failure), and `stats` (`prompt_tokens`/`completion_tokens`/`total_tokens`/`latency_s`/`model`, when the server reports usage). Wire only the ports you need rather than parsing `out`'s shape downstream.
- **ConstantValueNode** – Emits one configured literal value with no metadata wrapper (a bare string/number/bool, not `{'value': ...}` the way most other source nodes emit). Config: `value` (the literal to emit), `repeat` (bool, default `false` – emit once and idle vs. re-emit periodically), `interval` (seconds between repeats, if `repeat` is on). Meant mainly as the source of an `attribute`-type edge, so it can feed a clean raw value (e.g. a real filepath) onto another node's config attribute – see [Details](#constantvalue).

## Media Nodes
Images, audio and video travel through a graph as **media items** (`MediaItem`): the payload plus its MIME type and metadata such as `filename`, `source_path`, `width`/`height` or `duration`. Large payloads live in a blob store on disk and only a reference moves through the wires. Text nodes see a short description like `<image/png 1920x1080 3.1MB>`, and the editor's live view shows a preview.

- **MediaFileInputNode** – Reads one file *as a whole* (unlike FileInputNode, which tails appended bytes) and emits it as a media item. The type comes from the file's magic bytes, then its extension. Config: `path` (also settable via an attribute wire; a missing file just waits), `emit_on` (`change`, the default – on start and whenever size/mtime change, or `start` – once per path), `poll_interval` (default `1.0`).
- **MediaFileOutputNode** – Writes every item to its *own* file (FileOutputNode appends everything to one) and emits `{path, mime, size}` on `out`. Accepts media items, raw bytes (type sniffed) and text. Config: `path_pattern` (default `<files_dir>/media/{node}_{index:05d}.{ext}`; fields `{node}`, `{index}`, `{ext}` from the MIME type, `{kind}`, `{stem}` – the source file name without extension – and `{timestamp}`), `overwrite` (default `false` – an existing file is never replaced; the index advances instead).
- **Uploads into a graph**: WebInputNode and ApiInputNode accept file uploads besides JSON. `curl -F file=@photo.jpg http://host:8080/api/<uri>` (multipart; other form fields end up in `meta.form`) or `curl --data-binary @clip.mp4 -H 'Content-Type: video/mp4' ...` emit one media item per file. `application/octet-stream` is sniffed; name it with `?filename=` or an `X-Filename` header. Limit: `max_upload_mb` node config, default `PSF_MAX_UPLOAD_MB` (100); larger uploads get HTTP 413.
- **Serving media out of a graph**: WebOutputNode serves the newest media item at `<path>/media` (and at `<path>` itself when `sse` is off). ApiOutputNode serves it at `/api/<uri>/media` (and at `/api/<uri>/raw` when `sse` is off), with the item's own Content-Type and `Range` support for audio/video seeking. The JSON routes carry a summary instead of the bytes.
- **Base64**: Base64EncodeNode encodes a media item's payload; `data_url: true` produces `data:<mime>;base64,...` (for LLM vision APIs, HTML, MQTT). Base64DecodeNode turns such a data URL back into a media item (`output: auto`); `output: text|bytes|media` forces the result type.

## Image Nodes
Need Pillow: `pip install 'pystreamflow[image]'`. Without it these node types are not registered; the editor greys them out with that hint, and a workflow using them is rejected with the same message. They work on media items of kind `image` and `video_frame`, pass anything else through unchanged, and on a corrupt image pass the original through while recording the error on the node. A transformed image keeps its source format when that is PNG, JPEG or WebP (anything else becomes PNG) and keeps the item's `meta`, with `width`/`height` updated.

- **ImageDecodeNode** – Checks that the item is a decodable image and fills in `width`, `height`, `mode` and `format`; corrects a wrong MIME type. The bytes stay untouched unless `auto_orient: true` rotates an EXIF-rotated photo upright.
- **ImageResizeNode** – `max_side` (longer side), or `width`/`height` (with both: fit inside when `keep_aspect`, the default, else stretch; with one: the other follows). `resample`: `lanczos` (default), `bicubic`, `bilinear`, `nearest`. Never enlarges unless `upscale: true`.
- **ImageCropNode** – `width` × `height` at `x`/`y`, or centered with `center: true`; clamped to the image.
- **ImageRotateNode** – `angle` degrees counter-clockwise (default 90; multiples of 90 are lossless), `expand` (default `true`), `fill` colour for new corners; `angle: exif` rotates upright by the EXIF tag.
- **ImageFlipNode** – `direction`: `horizontal` (default), `vertical`, `both`.
- **ImageConvertNode** – `format`: `keep`, `png`, `jpeg`, `webp`, `gif`, `bmp`, `tiff`; `quality` 1–100 (JPEG/WebP, default 85); `mode`: `keep`, `RGB`, `RGBA`, `L` (WebP stores grayscale as RGB). Transparency is flattened onto white where the target can't keep it.
- **ImageFilterNode** – `filter`: `none`, `blur`, `gaussian_blur`, `box_blur`, `sharpen`, `unsharp_mask`, `edges`, `edge_enhance`, `contour`, `emboss`, `smooth`, `detail` (`radius` for the blur/unsharp ones, default 2); plus `brightness`, `contrast`, `saturation`, `sharpness` factors (1.0 = unchanged).
- **ImageInfoNode** – Emits a plain dict instead of the image: `mime`, `kind`, `size`, `width`, `height`, `mode`, `format`, `frames`, `has_alpha`, `meta` and `exif` (tag name → value; `exif: false` to omit) – for logic, compare, template and JSON nodes.
- **ImageThumbnailNode** – Fits into `max_side` × `max_side` (default 256) and encodes compactly: `format` `webp` (default), `jpeg` or `png`, `quality` (default 80).
- **Vision with LMStudioNode** – An image on its `in` port (alone, as a list, or as `{"image": ..., "prompt": "..."}`) is sent to a vision model as an `image_url` data URL. The question is the dict's `prompt`, else the latest value on the node's `prompt` input port, else its `image_prompt` config ("Describe this image."). Resize first – large images cost many tokens.

## Audio Nodes
Need numpy + soundfile: `pip install 'pystreamflow[audio]'` (greyed out in the editor otherwise). soundfile reads and writes WAV, FLAC, OGG and MP3; AAC/M4A, Opus output and the audio track of a video additionally need the `ffmpeg` executable on `PATH` (or `PSF_FFMPEG`).

Audio travels as media items of kind `audio` (a whole clip) or `audio_chunk` (a slice of a stream). Chunks are small 16-bit WAV files, so each one previews in the live view; their `meta` has `sample_rate`, `channels`, `frames`, `duration`, `pts` (seconds since the stream start), `index` and `last` (true on the final chunk). The processing nodes (resample, gain, normalize) always output WAV – compress again with AudioEncodeNode.

- **AudioDecodeNode** – Decodes an audio file, or a video's audio track, to PCM. `chunk_ms: 0` (default) emits the whole clip; `> 0` emits `audio_chunk` items of that length, read block by block so long files don't need to fit in memory.
- **AudioEncodeNode** – Collects audio into a file: `format` `wav` (default), `flac`, `ogg`, `mp3`, or `m4a`/`opus` via ffmpeg; `bitrate` for the ffmpeg formats (default `128k`). A file is finished every `segment_s` seconds (0 = off), when a whole `audio` item or a chunk marked `last` arrives, when anything arrives on the `flush` input port, or after `flush_idle_s` seconds without input (default 2; 0 = never). A change of sample rate or channel count also starts a new file.
- **AudioResampleNode** – `sample_rate` (e.g. `16000` for speech recognition) and/or `channels` (`1` = mono, `2` = stereo). Uses a windowed-sinc low-pass plus linear interpolation – fine for speech and monitoring.
- **AudioGainNode** – `gain_db` (+6 ≈ double, −6 ≈ half); clips at full scale.
- **AudioNormalizeNode** – Scales each item to `target_db`: `mode` `peak` (default, −1 dBFS) or `rms` (default −20 dBFS), gain capped at `max_gain_db` (default 30). Works per item – use it on clips or segments, not short chunks.
- **AudioLevelNode** – Measures each item/chunk: `out` gets `{rms, peak, rms_db, peak_db, silent, duration, pts}`, the `rms_db` and `peak_db` ports carry the bare numbers for CompareNode/TriggerThresholdNode. `silent` means `rms_db < silence_db` (default −50).
- **AudioSegmentNode** – Cuts a stream into `audio` segments (WAV, with `pts` and `index`). `mode: silence` (default) cuts where the level stays below `threshold_db` (default −40) for `min_silence_ms` (default 500), judged in 20 ms windows; silence is trimmed to `pad_ms` (default 200), segments shorter than `min_segment_s` (0.3) are dropped as noise and none exceeds `max_segment_s` (30). `mode: time` cuts every `segment_s` (default 10). Flushes like AudioEncodeNode (`flush` port, `last` chunk, `flush_idle_s`).
- **SpeechToTextNode** – Transcribes an audio (or video) item. `backend: api` (default) posts it to an OpenAI-compatible `<base_url>/audio/transcriptions` (OpenAI, speaches/faster-whisper-server, LocalAI – LM Studio has no such endpoint): `base_url` (default `http://localhost:8000/v1`), `model` (default `whisper-1`), `api_key`, `timeout` (default 300 s). `backend: local` runs faster-whisper in-process (`pip install 'pystreamflow[stt]'`): `model` (`small` default, `medium`, `large-v3`, or a path), `device` (`auto`/`cpu`/`cuda`), `compute_type`, `beam_size`. Both take `language` (empty = detect) and `prompt` (vocabulary hint). Outputs: `out` (text), `details` (`text`, `language`, `duration`, `segments`, `pts`, `source`), `errors`. Typical chain: AudioDecode (chunks) → AudioSegment → AudioResample (16000, mono) → SpeechToText.

## Output Nodes
- **FileOutputNode** – Appends items to a file.
- **LogOutputNode** – Logs items to console / file.
- **JSONOutputNode** – Serializes items to JSON.
- **HttpPostNode** – Sends each incoming item as a real outbound HTTP request to an external web server (the client-side counterpart to ApiInputNode/WebInputNode, which only ever *receive* requests into a workflow). Config: `url` (also updatable live via a regular config/attribute edge, so an upstream node can retarget it between runs), `method` (default `POST`; any verb is accepted, uppercased automatically), `content_type` (`json` default - a dict/list goes as-is, anything else is wrapped as `{"value": item}` so the body is always well-formed; or `form`, or `text` for the raw string/bytes), `headers` (extra headers, merged in), `auth_token` (optional - adds `Authorization: Bearer <auth_token>` unless `headers` already sets its own), `timeout` (read-leg seconds, default `30`; connect/write/pool stay fixed at a short `10s` so an unreachable server fails fast - same split LMStudioNode's own `timeout` uses). A non-2xx response is treated as a failure the same as a connection error. Emits to five ports: `out` (everything in one item), `request` (echoes what was sent, fires unconditionally), `response`/`stats` (body, and `status_code`/`latency_s`/`url`, only on a 2xx), and `errors` (just the error string, on any failure - the response body is still attached on `out` when the failure was itself an HTTP error status, so you can see what the server said).

## Modifier Nodes
- **GrepNode** – Filters lines by regex.
- **MergeNode** – Merges multiple input streams.
- **ForkNode** – Duplicates stream to multiple outputs.
- **TemplateNode** – Applies Jinja2-like templates.
- **JSONModifyNode** – Mutates JSON objects via config mapping.
- **EncodingConvertNode** – Converts between encodings.
- **RoundRobinNode** – Distributes one input stream across a fixed number of numbered outputs (`out0`, `out1`, ...) in strict rotating order: the 1st item received goes to `out0`, the 2nd to `out1`, ... the `count`-th wraps back around to `out0` again. The cycle position advances once per item regardless of whether that item's turn lands on a currently-wired output - it's a fixed turn-taking schedule, not "whichever outputs happen to be connected." Config: `count` (default `3`, 1-8) – how many output ports exist; the "+ output"/"− output" buttons on the node body grow or shrink `count` and its matching port together, the same pattern **SplitByValueNode**'s "+ value / output" uses. Send it a `reset` control action to restart the cycle at `out0`. Unlike **ForkNode** (duplicates to every wired output at once) or **SplitByValueNode** (routes by the item's own value), RoundRobinNode never looks at the item itself - purely turn-taking by arrival order, the classic round-robin load-balancing pattern.

## Logic Nodes
- **AndNode**, **OrNode**, **NotNode**, **XorNode**, **NandNode**, **NorNode**, **XnorNode** – Boolean operations.
- **CompareNode** – Compares two inputs.
- **MathNode** – Basic math operations.

## Numeric Nodes
- **NumericAddNode**, **NumericSubNode**, **NumericMulNode**, **NumericDivNode**, **NumericModNode**, **NumericPowNode**, **NumericMinNode**, **NumericMaxNode**, **NumericClampNode**, **NumericRoundNode**, **NumericAbsNode**

## Text Nodes
- **TextUpperNode**, **TextLowerNode**, **TextTrimNode**, **TextReplaceNode**, **TextSubstringNode**, **TextReverseNode**, **TextTitleNode**, **TextStripNode**, **TextSplitNode**, **TextJoinNode**

## Line / Token Nodes
- **LineSplitterNode** – Splits text into lines.
- **TokenizerNode** – Tokenizes text by delimiter.
- **LineBufferNode** – Buffers lines with configurable size.
- **TrimStringNode** – Trims whitespace.

## Subgraph & Utility
- **SubgraphNode** – Embeds a reusable workflow.
- **UrlInputNode** – Polls HTTP URLs via GET/POST.

## Constant Value

### ConstantValueNode
Emits `config['value']` exactly as configured, once immediately at start (or repeatedly every `interval` seconds if `repeat` is set) – with no `{'value': ...}` wrapper the way `ListStringsNode`/`GeneratorInputNode` add. This is the recommended source for wiring a clean, literal value (a real filepath, a hostname, a threshold) onto another node's config attribute via an `attribute`-type edge, since most other source node types wrap their real payload in a metadata dict that an attribute wire would otherwise deliver whole.

## Timer Node

### TimerNode
Emits a periodic tick payload with counter and timestamp. Config: `interval` seconds, `emit_payload`.

### ListStringsNode
Emits strings from a configured list sequentially. Config: `strings` – list or comma-separated string, `interval` seconds between emits, `loop` boolean to repeat. Emits `{'value': str, 'index': n}`.

### DisplayNode
Receives items, prints them to console with an optional prefix/suffix, and forwards them downstream. Config: `prefix` (text placed directly before the item), `suffix` (text placed directly after it) - both default to empty, and an empty value adds nothing at all (no brackets or other wrapping are ever added around the item). In the editor, the node's own live-view panel shows a word-wrapped, multi-line history of recently received items (in monospace, terminal-style text) rather than a single truncated line - drag the node's corner to resize it taller and the live-view panel grows with it, showing more at once. It also fetches more history than the default panel size can show at once (40 past items); two small buttons below the panel, "▲ older" and "▼ newer", page back and forth through that backlog - a small "▲▼ buttons scroll history" hint, replaced by a "12/40" position readout once you've scrolled, marks that this is possible. (Scrolling used to be wheel-driven - hovering the panel and using the mouse wheel - but on a DisplayNode resized large the panel covers most of the node, so a wheel anywhere near it got trapped scrolling the log instead of zooming the graph; the buttons replace that entirely, so the mouse wheel now always zooms the graph, everywhere, regardless of node size.) Because the panel is drawn on a canvas rather than as real page text, its content can't be click-drag selected or highlighted directly - a third button, "📋 copy visible", copies whatever's currently shown (respecting scroll position) to the clipboard in one click, so you can paste it into a text editor or chat box to select or highlight parts of it there.

### TableNode
Accumulates incoming items into a table and periodically emits the full table. Config: `columns` list, `max_rows`, `emit_interval`. Emits `{'columns':..., 'rows':[...]}`.

## Trigger System
Nodes support a `control` input port for runtime start/stop/pause/resume/step/emit/reset control. A control edge is drawn the same way as any other wire, but only ever from a Trigger-family node's output (or by dragging directly onto a target's reserved `control` slot) - it never competes with that target's ordinary data wiring.

A control input can have more than one source wired to it at once - two (or more) Trigger nodes can each independently control the same target, and each source's commands are delivered regardless of what any other source is doing. (Earlier versions of PyStreamFlow silently let a second control edge onto the same target steal control away from the first, with no error - if you're looking at an old workflow where a second trigger seemed to have no effect, that's why; it now just works.)

### TriggerNode
Emits a control command `{'action':'start|stop|pause|resume|step|emit|reset','target':node_id}` when it receives data. Config: `action`, `target_node_id`, `delay`. `action` is a free-form string, so any control action `handle_control` understands - including `reset` - can be sent this way.

### TimerTriggerNode
Periodically emits trigger actions. Config: `interval`, `action`, `target_node_id`.

### TriggerOnNode
On any input, emits `start` to target node. Config: `target_node_id`.

### TriggerOffNode
On any input, emits `stop` to target node. Config: `target_node_id`.

### TriggerPauseNode
On any input, emits `pause` to target node. Config: `target_node_id`.

All nodes handle control messages via `handle_control` and can be wired via the `control` input port.

### Reset
Every node supports a `reset` control action, alongside start/stop/pause/resume/step/emit - send it the same way (a Trigger node's `action` config set to `reset`, `POST /nodes/{id}/reset`, the `node_reset` MCP tool, or the "♻ Reset" item on a node's right-click menu in the editor). Reset clears a node's own accumulated stats/error bookkeeping (and, for a handful of node types that keep real accumulated data - **StackNode**, **FIFOQueueNode**, **LIFOQueueNode**, **TableNode**, **LineBufferNode** - the stack/queue/table/buffer contents themselves) without stopping the node, restarting it, or re-running its setup. Use it to clear a node's counters or contents while leaving it running, as opposed to `stop`/`start`, which tears the node down and brings it back up from scratch.

## General Help
All node parameters are exposed as API endpoints `GET /nodes/{id}/config` and editable via the Inspector. Parameters hidden by default start with `_`.

For CLI integration see `pystreamflow --help` (and `pystreamflow <command> --help` for a specific command, including which ones need an API key - see the [Authentication](user_guide.md#authentication) section of the user guide).

---

*Online docs are served at `/docs/` in the PyStreamFlow web UI.*
