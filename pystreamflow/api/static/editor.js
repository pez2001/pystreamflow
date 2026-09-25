/* PyStreamFlow Node Editor - ComfyUI-style rewrite (Phase 7)
 *
 * Built on litegraph.js (vendored under ./vendor/, MIT license - see
 * vendor/LITEGRAPH_LICENSE.txt) instead of the old hand-rolled DOM/SVG
 * canvas, per the user's explicit request to "look and feel more like
 * ComfyUI" (which is itself built on litegraph.js) and their choice, when
 * asked, to accept a real dependency rather than hand-roll pan/zoom/
 * noodles/minimap again, and to go for the "full ComfyUI feel" (inline
 * per-node widgets, search-to-add, collapsible nodes, minimap) rather
 * than a visual-only reskin of the old editor.
 *
 * This file preserves every piece of backend integration behaviour the
 * old pystreamflow/api/ui.html had - it is a rewrite of the *editor*, not
 * of the client/server contract:
 *   - GET /node-schema, /config-schema drive real port names and field
 *     widgets (core/port_schema.py, core/config_schema.py).
 *   - Edge "kind" (data/control/endpoint/attribute) and its priority
 *     rules (attribute target > Trigger-family source > manual override
 *     toggles > default data) are exactly what onmouseup used to compute
 *     in the old UI - see PSFNode.prototype.onConnectionsChange below.
 *   - Attribute-target handles (a value wired straight onto a config
 *     attribute - core/node.py's set_attribute()/Engine._pump_attribute())
 *     are modelled as ordinary input slots whose name is the attribute
 *     name, kept in sync with config.attributes (see syncAttributeSlots).
 *   - Run/Stop/Pause route through the Session/Engine backend exactly as
 *     before (POST /workflows -> POST /sessions -> /sessions/{id}/start,
 *     resuming a paused session in place rather than rebuilding it).
 *   - Import/Export/Load Demo/Load Session all still round-trip through
 *     POST /parse_yaml, matching the backend's real YAML shape.
 *   - Node templates are still persisted to localStorage under
 *     'psf_node_templates'.
 *   - The node catalog (nodeTypes/nodeGroups/nodeDocs) is carried over
 *     verbatim from the old ui.html - it's curated content, not something
 *     to regenerate from scratch.
 */
(function () {
  'use strict';

  // ------------------------------------------------------------------
  // Shared API-key auth (task: "add authentication"). The backend
  // (pystreamflow/core/auth.py + api/server.py's _require_api_key
  // middleware) now requires an 'Authorization: Bearer <key>' or
  // 'X-API-Key' header on every route except this page's own shell/
  // static assets and /health//version - and on by default: even if
  // the operator never sets PSF_API_KEY, the server generates one on
  // startup rather than ever running unauthenticated. This is the
  // other half: without it, every fetch() below (list nodes, run,
  // sessions, config edits, ...) would start failing with 401 the
  // instant auth is enabled, with no way for a person using this page
  // to ever supply the key.
  //
  // The key is kept in localStorage - a normal per-browser setting for
  // a real served app page, not the in-memory-only restriction that
  // applies to throwaway in-conversation preview artifacts - so it
  // survives a reload. Attached to every request by wrapping
  // window.fetch exactly once here, rather than hand-editing each of
  // the ~20 existing fetch() call sites below (and remembering to do
  // the same for every future one). A 401 response - wrong/missing key
  // - triggers a one-time prompt modal automatically; the caller's own
  // existing `if (data.error) ...` handling (see fetchJSON below)
  // still shows the server's own error text alongside it.
  let _psfApiKey = '';
  try { _psfApiKey = localStorage.getItem('psf_api_key') || ''; } catch (e) { /* private/blocked storage - session-only */ }

  const _psfNativeFetch = window.fetch.bind(window);
  window.fetch = function (input, init) {
    init = init || {};
    const headers = new Headers(init.headers || {});
    if (_psfApiKey && !headers.has('Authorization') && !headers.has('X-API-Key')) {
      headers.set('Authorization', `Bearer ${_psfApiKey}`);
    }
    init.headers = headers;
    return _psfNativeFetch(input, init).then((res) => {
      if (res.status === 401) promptForApiKey();
      return res;
    });
  };

  function setApiKey(key) {
    _psfApiKey = (key || '').trim();
    try { localStorage.setItem('psf_api_key', _psfApiKey); } catch (e) { /* private/blocked storage */ }
  }

  function promptForApiKey() {
    // Avoid stacking a second prompt if several requests 401 in a burst
    // (e.g. several nodes' periodic polls all in flight at once).
    if (promptForApiKey._open) return;
    promptForApiKey._open = true;
    const overlay = openModal('API Key Required', `
      <p class="muted">This PyStreamFlow server requires an API key on every request. Find it in the server's startup log, in <code>data/api_key.txt</code> (or wherever <code>PSF_API_KEY_FILE</code> points), or in whatever value <code>PSF_API_KEY</code> was set to.</p>
      <input type="password" id="apiKeyInput" placeholder="API key" autocomplete="off" style="width:100%;background:#0f172a;color:#e2e8f0;border:1px solid #334155;border-radius:6px;padding:8px;font-family:monospace">
      <div class="modal-actions">
        <button id="apiKeySave">Save</button>
      </div>
    `);
    const dismiss = () => { promptForApiKey._open = false; closeModal(); };
    overlay.querySelector('.modal-close').onclick = dismiss;
    overlay.onclick = (e) => { if (e.target === overlay) dismiss(); };
    const input = overlay.querySelector('#apiKeyInput');
    if (input) input.focus();
    const save = () => {
      setApiKey(input.value);
      promptForApiKey._open = false;
      closeModal();
      // Bug fix: this modal only ever appears because init()'s own very
      // first /node-schema and /config-schema fetches 401'd (no key yet)
      // and were swallowed by their own `.catch(() => ({}))` - leaving
      // the module-level portSchema/configSchema permanently stuck at {}
      // for the rest of the page's life, since nothing ever retried them.
      // rebuildPorts() falls back to a generic {inputs:['in'],
      // outputs:['out']} for any type missing from portSchema - so every
      // node created for the rest of the session (not just DYNAMIC-input
      // types like MergeNode/AndNode/ForkNode) silently got the wrong
      // ports, and psfDynamicInputs/psfDynamicOutputs came out false for
      // all of them - which is exactly why their "+ input"/"+ output"
      // (and "+ port"/"+ value output") buttons never appeared: those
      // buttons are only ever added when rebuildPorts() determined the
      // node's schema was genuinely dynamic. Only affects a cold start -
      // once a key is saved once, every later page load already carries
      // it on the very first fetch and never hits this path at all -
      // which is why it went unnoticed until a fresh browser/profile hit
      // it. Re-fetching both schemas now, the moment a key is saved,
      // means every node created from this point on (including the very
      // next one) gets its real schema - rebuildPorts() reads the
      // module-level portSchema/configSchema live on every call, so
      // nothing else needs to change for this to take effect immediately.
      Promise.all([
        fetchJSON('/node-schema').catch(() => null),
        fetchJSON('/config-schema').catch(() => null),
        fetchJSON('/node-availability').catch(() => null),
      ]).then(([ps, cs, av]) => {
        if (ps) portSchema = ps;
        if (cs) configSchema = cs;
        if (av && av.unavailable) { unavailableTypes = av.unavailable; buildPalette(); }
        toast(ps && cs ? 'API key saved.' : 'API key saved, but the node schema still failed to load - check the key and reload.');
      });
    };
    overlay.querySelector('#apiKeySave').onclick = save;
    if (input) input.addEventListener('keydown', (e) => { if (e.key === 'Enter') save(); });
  }

  // ------------------------------------------------------------------
  // Node catalog (verbatim from the old ui.html - see module docstring)
  // ------------------------------------------------------------------
  const nodeTypes = {
    WebInput: { type: 'WebInputNode', label: 'Web Input', icon: '🌐' },
    ApiInput: { type: 'ApiInputNode', label: 'API Input', icon: '🔌' },
    FileInput: { type: 'FileInputNode', label: 'File Input', icon: '📄' },
    DirectoryInput: { type: 'DirectoryInputNode', label: 'Directory Input', icon: '📁' },
    JSONInput: { type: 'JSONInputNode', label: 'JSON Input', icon: '{}' },
    LMStudio: { type: 'LMStudioNode', label: 'LM Studio', icon: '🤖' },
    ScriptInput: { type: 'ScriptInputNode', label: 'Script Input', icon: '📜' },
    MQTTInput: { type: 'MQTTInputNode', label: 'MQTT Input', icon: '📡' },
    ConstantValue: { type: 'ConstantValueNode', label: 'Constant Value', icon: '📌' },
    MQTTOutput: { type: 'MQTTOutputNode', label: 'MQTT Output', icon: '📡' },
    ScriptedOutput: { type: 'ScriptedOutputNode', label: 'Scripted Output', icon: '🖋️' },
    WebOutput: { type: 'WebOutputNode', label: 'Web Output', icon: '🌐' },
    WebOutputJSON: { type: 'WebOutputJSONNode', label: 'Web Output JSON', icon: '🌐' },
    ApiOutput: { type: 'ApiOutputNode', label: 'API Output', icon: '🔌' },
    FileOutput: { type: 'FileOutputNode', label: 'File Output', icon: '💾' },
    LogOutput: { type: 'LogOutputNode', label: 'Log Output', icon: '📝' },
    JSONOutput: { type: 'JSONOutputNode', label: 'JSON Output', icon: '{}' },
    Grep: { type: 'GrepNode', label: 'Grep', icon: '🔍' },
    Merge: { type: 'MergeNode', label: 'Merge', icon: '🔀' },
    Fork: { type: 'ForkNode', label: 'Fork', icon: '🍴' },
    Template: { type: 'TemplateNode', label: 'Template', icon: '📐' },
    JSONModify: { type: 'JSONModifyNode', label: 'JSON Modify', icon: '✏️' },
    And: { type: 'AndNode', label: 'And', icon: '✅' },
    Or: { type: 'OrNode', label: 'Or', icon: '🔀' },
    Not: { type: 'NotNode', label: 'Not', icon: '🚫' },
    Xor: { type: 'XorNode', label: 'Xor', icon: '⊻' },
    Nand: { type: 'NandNode', label: 'Nand', icon: '⊼' },
    Nor: { type: 'NorNode', label: 'Nor', icon: '⊽' },
    Xnor: { type: 'XnorNode', label: 'Xnor', icon: '⊻' },
    Compare: { type: 'CompareNode', label: 'Compare', icon: '⚖️' },
    Math: { type: 'MathNode', label: 'Math', icon: '➕' },
    NumAdd: { type: 'NumericAddNode', label: 'Num Add', icon: '➕' },
    NumSub: { type: 'NumericSubNode', label: 'Num Sub', icon: '➖' },
    NumMul: { type: 'NumericMulNode', label: 'Num Mul', icon: '✖️' },
    NumDiv: { type: 'NumericDivNode', label: 'Num Div', icon: '➗' },
    NumMod: { type: 'NumericModNode', label: 'Num Mod', icon: '≡' },
    NumPow: { type: 'NumericPowNode', label: 'Num Pow', icon: 'ʸ' },
    NumMin: { type: 'NumericMinNode', label: 'Num Min', icon: '↓' },
    NumMax: { type: 'NumericMaxNode', label: 'Num Max', icon: '↑' },
    NumClamp: { type: 'NumericClampNode', label: 'Num Clamp', icon: '⎇' },
    NumRound: { type: 'NumericRoundNode', label: 'Num Round', icon: '◐' },
    NumAbs: { type: 'NumericAbsNode', label: 'Num Abs', icon: '|x|' },
    TextUpper: { type: 'TextUpperNode', label: 'Text Upper', icon: 'A' },
    TextLower: { type: 'TextLowerNode', label: 'Text Lower', icon: 'a' },
    TextTrim: { type: 'TextTrimNode', label: 'Text Trim', icon: '✂️' },
    TextReplace: { type: 'TextReplaceNode', label: 'Text Replace', icon: '🔁' },
    TextSubstring: { type: 'TextSubstringNode', label: 'Text Substr', icon: '📄' },
    TextReverse: { type: 'TextReverseNode', label: 'Text Reverse', icon: '↔️' },
    TextTitle: { type: 'TextTitleNode', label: 'Text Title', icon: '🔤' },
    TextStrip: { type: 'TextStripNode', label: 'Text Strip', icon: '🧹' },
    TextSplit: { type: 'TextSplitNode', label: 'Text Split', icon: '✂️' },
    TextJoin: { type: 'TextJoinNode', label: 'Text Join', icon: '🔗' },
    Encoding: { type: 'EncodingConvertNode', label: 'Encoding Conv', icon: '🔤' },
    Subgraph: { type: 'SubgraphNode', label: 'Subgraph', icon: '🗂️' },
    LineSplitter: { type: 'LineSplitterNode', label: 'Line Splitter', icon: '↩️' },
    Tokenizer: { type: 'TokenizerNode', label: 'Tokenizer', icon: '🔤' },
    LineBuffer: { type: 'LineBufferNode', label: 'Line Buffer', icon: '📦' },
    TrimString: { type: 'TrimStringNode', label: 'Trim String', icon: '✂️' },
    ListStrings: { type: 'ListStringsNode', label: 'List Strings', icon: '📝' },
    Table: { type: 'TableNode', label: 'Table', icon: '📊' },
    Display: { type: 'DisplayNode', label: 'Display', icon: '🖥️' },
    Timer: { type: 'TimerNode', label: 'Timer', icon: '⏱️' },
    Trigger: { type: 'TriggerNode', label: 'Trigger', icon: '⚡' },
    TimerTrigger: { type: 'TimerTriggerNode', label: 'Timer Trigger', icon: '⏱️' },
    TriggerOn: { type: 'TriggerOnNode', label: 'Trigger On', icon: '▶️' },
    TriggerOff: { type: 'TriggerOffNode', label: 'Trigger Off', icon: '⏹️' },
    TriggerPause: { type: 'TriggerPauseNode', label: 'Trigger Pause', icon: '⏸️' },
    TriggerIf: { type: 'TriggerIfNode', label: 'Trigger If', icon: '🔍' },
    TriggerThreshold: { type: 'TriggerThresholdNode', label: 'Trigger Threshold', icon: '🎯' },
    TriggerDebounce: { type: 'TriggerDebounceNode', label: 'Trigger Debounce', icon: '⏳' },
    TriggerPulse: { type: 'TriggerPulseNode', label: 'Trigger Pulse', icon: '💓' },
    TriggerToggle: { type: 'TriggerToggleNode', label: 'Trigger Toggle', icon: '🔀' },
    Stack: { type: 'StackNode', label: 'Stack', icon: '📚' },
    FIFOQueue: { type: 'FIFOQueueNode', label: 'FIFO Queue', icon: '➡️' },
    LIFOQueue: { type: 'LIFOQueueNode', label: 'LIFO Queue', icon: '⬇️' },
    Clock: { type: 'ClockNode', label: 'Clock', icon: '🕒' },
    HTMLScraper: { type: 'HTMLScraperNode', label: 'HTML Scraper', icon: '🕸️' },
    Base64Decode: { type: 'Base64DecodeNode', label: 'Base64 Decode', icon: '🔓' },
    Base64Encode: { type: 'Base64EncodeNode', label: 'Base64 Encode', icon: '🔒' },
    UserPrompt: { type: 'UserPromptNode', label: 'User Prompt', icon: '💬' },
    RollingWindowBuffer: { type: 'RollingWindowBufferNode', label: 'Rolling Window Buffer', icon: '🪟' },
    UrlInput: { type: 'UrlInputNode', label: 'URL Input', icon: '🌐' },
    // These 11 backend node types (of the 90 the server registers) were
    // simply missing from this catalog - not hidden-by-default, not
    // stub-flagged, just entirely absent, so there was no way to add one
    // to a graph through the editor at all (no palette entry, no
    // search-box hit) even though every one of them is a real,
    // YAML/MCP/CLI-loadable node type. Found during a full audit of
    // whether every node type is "actually usefully connected into the
    // framework" - being unreachable from the visual editor is as
    // disconnected as it gets. Restored here; nodeDocs below is honest
    // about which of these are genuinely functional vs. currently
    // simulated placeholders (confirmed by their own test names/asserts).
    Generator: { type: 'GeneratorInputNode', label: 'Generator', icon: '🔢' },
    SocketInput: { type: 'SocketInputNode', label: 'Socket Input', icon: '🔌' },
    UserInput: { type: 'UserInputNode', label: 'User Input', icon: '🙋' },
    LogInput: { type: 'LogInputNode', label: 'Log Input', icon: '📃' },
    ProcessInput: { type: 'ProcessInputNode', label: 'Process Input', icon: '⚙️' },
    PythonScriptInput: { type: 'PythonScriptInputNode', label: 'Python Script Input', icon: '🐍' },
    ShellInput: { type: 'ShellInputNode', label: 'Shell Input', icon: '💻' },
    ProcessOutput: { type: 'ProcessOutputNode', label: 'Process Output', icon: '⚙️' },
    SocketOutput: { type: 'SocketOutputNode', label: 'Socket Output', icon: '🔌' },
    JSONExtract: { type: 'JSONExtractNode', label: 'JSON Extract', icon: '🔎' },
    ModifierScript: { type: 'ScriptNode', label: 'Script', icon: '📜' },
    // Feature request round: split-by-value routing, an MCP client, a
    // cron-scheduled source, a variable-input sync barrier, a queue+pop
    // gate, and a visual LED-activity relay.
    SplitByValue: { type: 'SplitByValueNode', label: 'Split by Value', icon: '🔀' },
    MCPClient: { type: 'MCPClientNode', label: 'MCP Client', icon: '🧩' },
    Cron: { type: 'CronNode', label: 'Cron', icon: '⏰' },
    SyncBarrier: { type: 'SyncBarrierNode', label: 'Sync Barrier', icon: '🚦' },
    QueueGate: { type: 'QueueGateNode', label: 'Queue Gate', icon: '🎟️' },
    LedActivity: { type: 'LedActivityNode', label: 'LED Activity', icon: '🔴' },
    // Feature request: "add a node to feed inputs to multiple outputs and
    // it cycles through all outputs (first input message gets to the
    // first output, the second incoming message goes to the second
    // output. if all outputs received one message start the cycle
    // again)" - see pystreamflow/nodes/round_robin_node.py.
    RoundRobin: { type: 'RoundRobinNode', label: 'Round Robin', icon: '🎠' },
    // Feature request: "add a node to post data to external webservers" -
    // see pystreamflow/nodes/http_post_node.py.
    HttpPost: { type: 'HttpPostNode', label: 'HTTP Post', icon: '📤' },
    MediaFileInput: { type: 'MediaFileInputNode', label: 'Media File Input', icon: '🖼️' },
    MediaFileOutput: { type: 'MediaFileOutputNode', label: 'Media File Output', icon: '🗂️' },
    ImageDecode: { type: 'ImageDecodeNode', label: 'Image Decode', icon: '🖼️' },
    ImageResize: { type: 'ImageResizeNode', label: 'Image Resize', icon: '↔️' },
    ImageCrop: { type: 'ImageCropNode', label: 'Image Crop', icon: '✂️' },
    ImageRotate: { type: 'ImageRotateNode', label: 'Image Rotate', icon: '🔄' },
    ImageFlip: { type: 'ImageFlipNode', label: 'Image Flip', icon: '🪞' },
    ImageConvert: { type: 'ImageConvertNode', label: 'Image Convert', icon: '🔁' },
    ImageFilter: { type: 'ImageFilterNode', label: 'Image Filter', icon: '🎚️' },
    ImageInfo: { type: 'ImageInfoNode', label: 'Image Info', icon: 'ℹ️' },
    ImageThumbnail: { type: 'ImageThumbnailNode', label: 'Image Thumbnail', icon: '🔳' },
    AudioDecode: { type: 'AudioDecodeNode', label: 'Audio Decode', icon: '🎵' },
    AudioEncode: { type: 'AudioEncodeNode', label: 'Audio Encode', icon: '💿' },
    AudioResample: { type: 'AudioResampleNode', label: 'Audio Resample', icon: '🎛️' },
    AudioGain: { type: 'AudioGainNode', label: 'Audio Gain', icon: '🔊' },
    AudioNormalize: { type: 'AudioNormalizeNode', label: 'Audio Normalize', icon: '📶' },
    AudioLevel: { type: 'AudioLevelNode', label: 'Audio Level', icon: '📊' },
    AudioSegment: { type: 'AudioSegmentNode', label: 'Audio Segment', icon: '✂️' },
    SpeechToText: { type: 'SpeechToTextNode', label: 'Speech to Text', icon: '🗣️' },
  };
  const nodeDocs = {
    WebInputNode: { desc: 'HTTP input via shared web server: a JSON object body is emitted as-is, a file upload (multipart/form-data, or a raw image/audio/video/octet-stream body) as one media item per file', url: '/docs/nodes/index.md#webinputnode' },
    ApiInputNode: { desc: 'REST input: uri maps to a real POST endpoint at /api/<uri> on the shared web server; emits each request\'s JSON body, or one media item per uploaded file (multipart/form-data or a raw image/audio/video/octet-stream body)', url: '/docs/nodes/index.md#apiinputnode' },
    ApiOutputNode: { desc: 'REST output: uri maps to /api/<uri> (JSON) plus /api/<uri>/raw (same data, unencapsulated; a media item is served as the file itself) and /api/<uri>/media (newest media item) on the shared web server; starts the server itself. Also has a graph-level "raw" output port (⇢ raw), wireable on the canvas, alongside the normal "out" port.', url: '/docs/nodes/index.md#apioutputnode' },
    FileInputNode: { desc: 'Poll file for new bytes', url: '/docs/nodes/index.md' },
    DirectoryInputNode: { desc: 'Watches a directory (path, optional recursive) and emits new files on `files`, new subdirectories on `dirs`. Optional `extensions`/`media_only` filter; `emit_as: media` emits whole files as media items instead of paths', url: '/docs/nodes/index.md' },
    MediaFileInputNode: { desc: 'Reads one image/audio/video file as a whole and emits it as a media item (emit_on: change = on start and whenever the file changes, start = once)', url: '/docs/nodes/index.md#media-nodes' },
    MediaFileOutputNode: { desc: 'Writes each item to its own file (path_pattern with {node} {index:05d} {ext} {kind} {stem} {timestamp}; default "<files_dir>/media/{node}_{index:05d}.{ext}") and emits {path, mime, size}', url: '/docs/nodes/index.md#media-nodes' },
    JSONInputNode: { desc: 'Reads newline-delimited JSON from a real source (source: "stdin", the default, or a file path to tail) and emits each parsed value', url: '/docs/nodes/index.md' },
    LMStudioNode: { desc: 'LM Studio OpenAI compatible LLM. Images on `in` are sent to vision models; the question comes from the `prompt` input port or image_prompt', url: '/docs/nodes/index.md' },
    ImageDecodeNode: { desc: 'Checks an item is a decodable image and fills in width/height/mode/format (auto_orient: rotate photos upright by EXIF)', url: '/docs/nodes/index.md#image-nodes' },
    ImageResizeNode: { desc: 'Scale to max_side, or width/height (fit inside with keep_aspect); resample nearest|bilinear|bicubic|lanczos; never enlarges unless upscale', url: '/docs/nodes/index.md#image-nodes' },
    ImageCropNode: { desc: 'Cut out width x height at x/y, or centered (center: true)', url: '/docs/nodes/index.md#image-nodes' },
    ImageRotateNode: { desc: 'Rotate by angle degrees counter-clockwise (exif = upright by EXIF tag)', url: '/docs/nodes/index.md#image-nodes' },
    ImageFlipNode: { desc: 'Mirror horizontally, vertically or both', url: '/docs/nodes/index.md#image-nodes' },
    ImageConvertNode: { desc: 'Re-encode as png/jpeg/webp/gif/bmp/tiff with quality; mode RGB/RGBA/L', url: '/docs/nodes/index.md#image-nodes' },
    ImageFilterNode: { desc: 'Blur, sharpen, edges, ... plus brightness/contrast/saturation/sharpness factors', url: '/docs/nodes/index.md#image-nodes' },
    ImageInfoNode: { desc: 'Emits a dict with size, dimensions, mode, format, alpha and EXIF - for logic/compare/JSON nodes', url: '/docs/nodes/index.md#image-nodes' },
    ImageThumbnailNode: { desc: 'Small preview (max_side, default 256) as WebP/JPEG/PNG', url: '/docs/nodes/index.md#image-nodes' },
    AudioDecodeNode: { desc: 'Audio file (or a video\'s audio track) to PCM: whole clip, or audio_chunk items of chunk_ms with pts', url: '/docs/nodes/index.md#audio-nodes' },
    AudioEncodeNode: { desc: 'Collects audio into wav/flac/ogg/mp3 (m4a/opus via ffmpeg) files - per segment_s, on the flush port, at a stream\'s last chunk or after flush_idle_s', url: '/docs/nodes/index.md#audio-nodes' },
    AudioResampleNode: { desc: 'Change sample_rate (e.g. 16000 for speech recognition) and/or channels (1 = mono)', url: '/docs/nodes/index.md#audio-nodes' },
    AudioGainNode: { desc: 'Change the level by gain_db', url: '/docs/nodes/index.md#audio-nodes' },
    AudioNormalizeNode: { desc: 'Scale each item to a peak or rms target_db (gain limited by max_gain_db)', url: '/docs/nodes/index.md#audio-nodes' },
    AudioLevelNode: { desc: 'RMS/peak level per item or chunk: dict on out, bare dB numbers on rms_db/peak_db (for Compare/TriggerThreshold)', url: '/docs/nodes/index.md#audio-nodes' },
    AudioSegmentNode: { desc: 'Cuts a stream into segments at silence (threshold_db, min_silence_ms) or every segment_s', url: '/docs/nodes/index.md#audio-nodes' },
    SpeechToTextNode: { desc: 'Transcribes audio to text: backend api (OpenAI-compatible /audio/transcriptions) or local (faster-whisper)', url: '/docs/nodes/index.md#audio-nodes' },
    ScriptInputNode: { desc: 'Periodic script execution input', url: '/docs/nodes/index.md#scriptinput' },
    FileOutputNode: { desc: 'Really appends each item to a real file on disk (path, default "<files_dir>/output.txt" - PSF_FILES_DIR, /app/files under docker-compose.yml)', url: '/docs/nodes/index.md' },
    LogOutputNode: { desc: 'Logs each item via Python logging AND to a real log file (file, default "<logs_dir>/pystreamflow.log" - PSF_LOGS_DIR, /app/logs under docker-compose.yml)', url: '/docs/nodes/index.md' },
    JSONOutputNode: { desc: 'Serialize to JSON', url: '/docs/nodes/index.md' },
    GrepNode: { desc: 'Filter by regex', url: '/docs/nodes/index.md' },
    MergeNode: { desc: 'Merge streams', url: '/docs/nodes/index.md' },
    ForkNode: { desc: 'Duplicate stream. Every normal output (out0, out1, ...) has a paired raw output (⇢ raw0, ⇢ raw1, ...) carrying the same duplicated item; "+ output"/"− output" grow or shrink both together.', url: '/docs/nodes/index.md' },
    TemplateNode: { desc: 'Apply templates', url: '/docs/nodes/index.md' },
    JSONModifyNode: { desc: 'Modify JSON objects', url: '/docs/nodes/index.md' },
    AndNode: { desc: 'Boolean AND', url: '/docs/nodes/index.md' },
    OrNode: { desc: 'Boolean OR', url: '/docs/nodes/index.md' },
    NotNode: { desc: 'Boolean NOT', url: '/docs/nodes/index.md' },
    NumericAddNode: { desc: 'Add numbers', url: '/docs/nodes/index.md' },
    TextUpperNode: { desc: 'Uppercase text', url: '/docs/nodes/index.md' },
    EncodingConvertNode: { desc: 'Convert encodings', url: '/docs/nodes/index.md' },
    SubgraphNode: { desc: 'Embed workflow', url: '/docs/nodes/index.md' },
    UrlInputNode: { desc: 'Poll URLs', url: '/docs/nodes/index.md' },
    TimerNode: { desc: 'Periodic tick emitter', url: '/docs/nodes/index.md#timer' },
    ListStringsNode: { desc: 'Emit strings from list', url: '/docs/nodes/index.md#liststrings' },
    TableNode: { desc: 'Accumulate rows into table', url: '/docs/nodes/index.md#table' },
    DisplayNode: { desc: 'Print and forward items', url: '/docs/nodes/index.md#display' },
    TriggerNode: { desc: 'Emit trigger action to target', url: '/docs/nodes/index.md#trigger' },
    TimerTriggerNode: { desc: 'Periodic trigger', url: '/docs/nodes/index.md#timer' },
    TriggerOnNode: { desc: 'Trigger start on input', url: '/docs/nodes/index.md#triggeron' },
    TriggerOffNode: { desc: 'Trigger stop on input', url: '/docs/nodes/index.md#triggeroff' },
    TriggerPauseNode: { desc: 'Trigger pause on input', url: '/docs/nodes/index.md#triggerpause' },
    TriggerIfNode: { desc: 'Trigger on condition match', url: '/docs/nodes/index.md#triggerif' },
    TriggerThresholdNode: { desc: 'Trigger after N inputs', url: '/docs/nodes/index.md#triggerthreshold' },
    TriggerDebounceNode: { desc: 'Debounce trigger events', url: '/docs/nodes/index.md#triggerdebounce' },
    TriggerPulseNode: { desc: 'Periodic pulse trigger', url: '/docs/nodes/index.md#triggerpulse' },
    TriggerToggleNode: { desc: 'Toggle action on each input', url: '/docs/nodes/index.md#triggertoggle' },
    StackNode: { desc: 'Push/pop stack', url: '/docs/nodes/index.md#stack' },
    FIFOQueueNode: { desc: 'First-in-first-out queue', url: '/docs/nodes/index.md#fifo' },
    LIFOQueueNode: { desc: 'Last-in-first-out queue', url: '/docs/nodes/index.md#lifo' },
    ClockNode: { desc: 'CPU-style ticking emitter', url: '/docs/nodes/index.md#clock' },
    HTMLScraperNode: { desc: 'Extract data from HTML with CSS selectors or regex', url: '/docs/nodes/index.md#htmlscraper' },
    Base64DecodeNode: { desc: 'Decode base64 encoded strings to text/bytes; a data: URL becomes a media item (output: auto|text|bytes|media)', url: '/docs/nodes/index.md#base64decode' },
    Base64EncodeNode: { desc: 'Encode text/bytes/media items to a base64 string (data_url: true for data:<mime>;base64,...)', url: '/docs/nodes/index.md#base64encode' },
    UserPromptNode: { desc: 'Pause flow and wait for user input via user port', url: '/docs/nodes/index.md#userprompt' },
    RollingWindowBufferNode: { desc: 'Rolling window history buffer with triggerable flush', url: '/docs/nodes/index.md#rollingwindowbuffer' },
    ConstantValueNode: { desc: 'Emits one configured literal value with no metadata wrapper - the clean raw-value source for attribute wiring', url: '/docs/nodes/index.md#constantvalue' },
    GeneratorInputNode: { desc: 'Emits an increasing counter value `count` times, then stops', url: '/docs/nodes/index.md' },
    SocketInputNode: { desc: 'Real TCP server; emits each line received from a connected client', url: '/docs/nodes/index.md' },
    UserInputNode: { desc: 'Real HTTP endpoint; emits whatever value a person POSTs to it', url: '/docs/nodes/index.md' },
    JSONExtractNode: { desc: 'Extract one dot-separated key path out of a parsed JSON item', url: '/docs/nodes/index.md' },
    LogInputNode: { desc: 'Attaches a real logging.Handler to a Python logger (logger_name, default root) and emits each real record it receives at/above `level`', url: '/docs/nodes/index.md' },
    ProcessInputNode: { desc: '⚠ Runs a real OS command (cmd) periodically via direct exec (no shell) and emits its actual stdout/stderr - cwd defaults to data_dir (PSF_DATA_DIR, /app/data under docker-compose.yml) - gate with PSF_ALLOW_SHELL_NODES=0 to disable', url: '/docs/nodes/index.md' },
    PythonScriptInputNode: { desc: '⚠ Executes real Python (script) via exec() periodically and emits its printed output or `result` value - gate with PSF_ALLOW_SCRIPT_NODES=0 to disable', url: '/docs/nodes/index.md' },
    ShellInputNode: { desc: '⚠ Runs a real shell command line (command) via /bin/sh -c periodically and emits its actual stdout/stderr - cwd defaults to data_dir (PSF_DATA_DIR, /app/data under docker-compose.yml) - gate with PSF_ALLOW_SHELL_NODES=0 to disable', url: '/docs/nodes/index.md' },
    ProcessOutputNode: { desc: '⚠ Pipes each incoming item to a real OS command\'s (cmd) stdin and emits its actual stdout - cwd defaults to data_dir (PSF_DATA_DIR, /app/data under docker-compose.yml) - gate with PSF_ALLOW_SHELL_NODES=0 to disable', url: '/docs/nodes/index.md' },
    SocketOutputNode: { desc: '⚠ mode "client" (default): opens a real outbound TCP connection to host:port for each incoming item and sends it - no port is listened on here. mode "server": this node itself listens on host:port (like Socket Input) and broadcasts each item to every connected client. Gate with PSF_ALLOW_SOCKET_NODES=0 to disable either mode.', url: '/docs/nodes/index.md' },
    ScriptNode: { desc: '⚠ Executes real Python (script) via exec() against each incoming item and emits its `result` - gate with PSF_ALLOW_SCRIPT_NODES=0 to disable', url: '/docs/nodes/index.md' },
    SplitByValueNode: { desc: 'Routes each item to a different output (out0, out1, ...) depending on its value (or one dot-path field of it, via `key`) - "+ value/output" grows `values` and its matching port together; anything unmatched goes to the always-present "default" output.', url: '/docs/nodes/index.md#splitbyvalue' },
    MCPClientNode: { desc: 'Calls a `tool` on an external Model Context Protocol server (any server speaking the same tools/call JSON-RPC shape this project\'s own MCP server does) for every incoming item, and emits the real result', url: '/docs/nodes/index.md#mcpclient' },
    CronNode: { desc: 'Emits a tick whenever the wall-clock time (UTC) matches a real 5-field cron expression - "run at 9am", not just "run every N seconds"', url: '/docs/nodes/index.md#cron' },
    SyncBarrierNode: { desc: 'Waits until every wired input has received a message, then releases them all at once onto their matching outputs; `flush` controls whether held values are cleared afterward (wait for a fresh full round) or kept (re-release on any single update)', url: '/docs/nodes/index.md#syncbarrier' },
    QueueGateNode: { desc: 'Queues everything received on `in`; every message on `pop` (its content is discarded) releases exactly one queued item, FIFO, onto `out`', url: '/docs/nodes/index.md#queuegate' },
    LedActivityNode: { desc: 'Relays every input straight to the output at the same index and blinks a little LED indicator on the canvas each time it does - a wiring/diagnostic gadget, not a data transform', url: '/docs/nodes/index.md#ledactivity' },
    RoundRobinNode: { desc: 'Distributes items across a fixed number of outputs (out0, out1, ...) in strict rotating order - 1st item to out0, 2nd to out1, ... wrapping back to out0 after `count` outputs; "+ output"/"− output" grow or shrink `count` and its matching port together', url: '/docs/nodes/index.md' },
    HttpPostNode: { desc: 'Sends each incoming item as a real outbound HTTP request (method, default POST) to an external `url` - json/form/text body, optional headers/bearer `auth_token`; `request` echoes what was sent, `response`/`stats` fire on success, `errors` on failure (connection error or non-2xx)', url: '/docs/nodes/index.md' },
  };
  const nodeGroups = {
    Inputs: ['WebInput', 'ApiInput', 'FileInput', 'DirectoryInput', 'JSONInput', 'LMStudio', 'UrlInput', 'ScriptInput', 'MQTTInput', 'ConstantValue', 'Generator', 'SocketInput', 'UserInput', 'LogInput', 'ProcessInput', 'PythonScriptInput', 'ShellInput'],
    Outputs: ['FileOutput', 'LogOutput', 'JSONOutput', 'Display', 'MQTTOutput', 'ScriptedOutput', 'WebOutput', 'WebOutputJSON', 'ApiOutput', 'ProcessOutput', 'SocketOutput', 'HttpPost'],
    Modifiers: ['Grep', 'Merge', 'Fork', 'Template', 'JSONModify', 'Encoding', 'JSONExtract', 'ModifierScript', 'SplitByValue', 'RoundRobin', 'MCPClient', 'LedActivity'],
    Logic: ['And', 'Or', 'Not', 'Xor', 'Nand', 'Nor', 'Xnor', 'Compare', 'Math'],
    Numeric: ['NumAdd', 'NumSub', 'NumMul', 'NumDiv', 'NumMod', 'NumPow', 'NumMin', 'NumMax', 'NumClamp', 'NumRound', 'NumAbs'],
    Text: ['TextUpper', 'TextLower', 'TextTrim', 'TextReplace', 'TextSubstring', 'TextReverse', 'TextTitle', 'TextStrip', 'TextSplit', 'TextJoin'],
    'Line/Token': ['LineSplitter', 'Tokenizer', 'LineBuffer', 'TrimString', 'ListStrings', 'Table'],
    Control: ['Timer', 'Trigger', 'TimerTrigger', 'TriggerOn', 'TriggerOff', 'TriggerPause', 'TriggerIf', 'TriggerThreshold', 'TriggerDebounce', 'TriggerPulse', 'TriggerToggle', 'Cron'],
    Media: ['MediaFileInput', 'MediaFileOutput', 'ImageDecode', 'ImageResize', 'ImageCrop', 'ImageRotate', 'ImageFlip', 'ImageConvert', 'ImageFilter', 'ImageInfo', 'ImageThumbnail', 'AudioDecode', 'AudioEncode', 'AudioResample', 'AudioGain', 'AudioNormalize', 'AudioLevel', 'AudioSegment', 'SpeechToText'],
    Advanced: ['Subgraph', 'Stack', 'FIFOQueue', 'LIFOQueue', 'Clock', 'HTMLScraper', 'Base64Decode', 'Base64Encode', 'UserPrompt', 'RollingWindowBuffer', 'SyncBarrier', 'QueueGate'],
  };

  const keyGroup = {};
  Object.entries(nodeGroups).forEach(([g, keys]) => keys.forEach((k) => { keyGroup[k] = g; }));
  const typeToKey = {};
  Object.entries(nodeTypes).forEach(([k, meta]) => { typeToKey[meta.type] = k; });

  // ------------------------------------------------------------------
  // Edge-kind visuals (mirrors the old SVG stroke/dash choices exactly)
  // ------------------------------------------------------------------
  // Phase 4 of the wire-kind-unification design (see
  // claude/design_unified_wire_kinds_plan.md): 'raw' gets its own real
  // wire color/dash now that it's a genuine per-wire delivery kind
  // (core/node.py's emit() actually unwraps for it - see Phase 2)
  // instead of just a second, identical-data output port. Deliberately
  // NOT reusing attribute's purple, despite both sharing the same
  // _clean_attribute_value() unwrap under the hood - conflating them
  // visually would undo the whole point of color-coding a wire's kind
  // at a glance. Matches RAW_PORT_COLOR below (the pre-existing color
  // for a port literally *named* 'raw', e.g. ApiOutputNode's own
  // hand-declared one) on purpose: same teal for the same underlying
  // "unencapsulated" concept, whether it shows up as a port name or a
  // wire kind.
  const KIND_COLORS = { data: '#3b82f6', control: '#f59e0b', endpoint: '#a855f7', attribute: '#a855f7', raw: '#14b8a6' };
  const KIND_DASH = { data: [], control: [6, 4], endpoint: [4, 4], attribute: [2, 3], raw: [8, 2, 2, 2] };
  const STATUS_COLORS = { idle: '#475569', active: '#22c55e', waiting: '#eab308', error: '#ef4444' };
  const TRIGGER_RE = /Trigger/;
  const DYNAMIC_SLOT_COUNT = 4;
  const DYNAMIC_SLOT_MAX = 8;
  const DYNAMIC_SLOT_MIN = 1;
  // Any output port literally named 'raw' or 'raw_<name>' gets this
  // distinct color/label so a "same data, unencapsulated" tap point reads
  // unambiguously as its own kind of port on the canvas, not just another
  // same-looking data slot. Before Phase 2 of the wire-kind-unification
  // design (see claude/design_unified_wire_kinds_plan.md), *every* fixed
  // output port got one of these generated automatically
  // (core/port_schema.py's now-deleted _add_raw_pairs()); Phase 2 retired
  // that entirely in favor of 'raw' as a per-wire delivery *kind* chosen
  // at wire-draw time (see KIND_COLORS.raw and getSlotMenuOptions below),
  // so a name matching this pattern in the schema now only ever means a
  // node type's own genuinely separate, hand-declared port - currently
  // just ApiOutputNode's 'raw' (see its own docstring/_OVERRIDES entry) -
  // not a generic auto-duplicate. ForkNode's numbered 'raw0'/'raw1'/...
  // (its own separate, untouched fan-out-multiplicity mechanism - see
  // psfPairedRawOutputs below) still matches this same pattern too.
  const RAW_PORT_COLOR = KIND_COLORS.raw;
  const RAW_PORT_RE = /^raw(_\w+|\d*)$/;
  // Feature flag, now scoped to exactly one thing: whether ForkNode's own
  // separate outN/rawN pairing (psfPairedRawOutputs below - a *different*
  // concern from Phase 2/4's per-wire raw kind, explicitly out of scope
  // for that design per its own "Fork's own pairing... is not touched by
  // this plan" note) renders its paired rawN ports. Direct feedback after
  // trying the old generalized-to-every-node raw port feature live: 'raw'
  // and 'out' were genuinely identical items on essentially every node
  // type back then (see e.g. ApiOutputNode's own docstring - "out and raw
  // currently carry identical items" - equally true for Fork's own
  // raw0/raw1/..., which still just duplicates today, unchanged by Phase
  // 2), so they just looked like a second, useless copy of the same port.
  // That reasoning no longer applies to the generic case at all (Phase 2
  // removed the schema-level auto-duplication outright, and Phase 4 below
  // replaced "a second named port" with "a kind you pick per wire"), but
  // it's still exactly as true as ever for Fork's own untouched mechanism
  // - left off here for the same reason it always was.
  const SHOW_RAW_PORTS = false;
  // How many past values DisplayNode's live view fetches to fill its
  // multi-line scrollback with (see addWidgetsForNode()'s psfType ===
  // 'DisplayNode' branch and pollNodeStatus() below) - deliberately more
  // than the panel's default minimum height could ever show at once, so
  // resizing the node taller (see DISPLAY_LIVE_VIEW_MIN_HEIGHT below) has
  // real backlog to reveal instead of just empty space.
  const DISPLAY_HISTORY_LINES = 40;
  // Minimum pixel height of DisplayNode's dedicated "live view" panel -
  // see addWidgetsForNode()'s psfType === 'DisplayNode' branch below. The
  // panel is resizable: dragging the node taller than its natural default
  // size (every PSFNode sets `this.resizable = true`) grows the live view
  // by exactly that much extra height rather than leaving it a fixed size
  // with dead space below it - see node._displayLiveViewBaseHeight below.
  const DISPLAY_LIVE_VIEW_MIN_HEIGHT = 70;

  // ------------------------------------------------------------------
  // Global editor state
  // ------------------------------------------------------------------
  let portSchema = {};
  // Node types this server can't run (optional dependency missing, e.g.
  // Pillow for the image nodes) -> install hint; see GET /node-availability.
  let unavailableTypes = {};
  let configSchema = {};
  let nodeTemplates = {};
  try { nodeTemplates = JSON.parse(localStorage.getItem('psf_node_templates') || '{}'); } catch (e) { nodeTemplates = {}; }
  let currentSessionId = null;
  const nodeProgressMap = new Map();
  const nodeLastSeen = {};
  const nodeBlinkUntil = {};
  // Feedback: "add a glowing border effect (just a flash) to nodes which
  // received input or emitted output further down the chain". Separate
  // from nodeBlinkUntil (the small status-dot flicker, and the manual-emit
  // button's own blink) - this drives a border glow drawn in
  // onDrawForeground below, and it also propagates: a node whose output
  // genuinely just changed schedules a delayed flash on every node wired
  // directly downstream of it (see propagateFlash()), so the glow visibly
  // travels along the wires the way the data itself does, not just
  // lighting up whichever single node happened to be polled.
  const nodeFlashUntil = {};
  // The last-seen /last-entry "signature" per node (see pollNodeStatus())
  // - lets a flash trigger only on genuinely *new* output, rather than
  // every poll tick re-triggering forever just because *some* item exists
  // (an unwired-then-never-changing last item would otherwise blink
  // indefinitely).
  const nodeLastItemSig = {};
  const psfNodesById = new Map(); // psfId -> LGraphNode
  let graph, graphcanvas, canvasEl;

  // Subgraph view/edit navigation stack (Request C9: "there should be
  // some way to view/edit subgraphs"). Each entry describes the subgraph
  // frame we're currently *inside* (empty at the top-level/main graph):
  // { path, node } - the subgraph's own workflow file path, and the
  // SubgraphNode instance (living in the *parent* graph, which
  // graphcanvas._graph_stack already remembers how to get back to via
  // LiteGraph's own built-in openSubgraph()/closeSubgraph()) we opened it
  // from. LiteGraph's stack holds the LGraph objects themselves; this one
  // holds the extra bookkeeping (which file to save back to, which node
  // to refresh/reselect) LiteGraph has no concept of.
  let subgraphNavStack = [];

  // rawArmed: null, or { nodeId, slot } naming the one output anchor
  // whose *next* wire should be tagged 'raw' instead of 'data' - see
  // getSlotMenuOptions()/onConnectionsChange() below. Deliberately not a
  // third persistent toggle alongside triggerMode/endpointMode: it's
  // scoped to one specific anchor and consumed by the very next wire
  // drawn from it either way, so unlike those two there's no state left
  // behind to forget to turn off.
  const state = { triggerMode: false, endpointMode: false, rawArmed: null };
  window.psfEditor = state; // read by PSFNode.prototype.onConnectionsChange below

  function genId() { return Math.random().toString(36).slice(2, 10); }

  function keyForType(psfType) {
    return typeToKey[psfType] || null;
  }
  function fullTypeForKey(key) {
    const group = keyGroup[key] || 'Other';
    return `psf/${group}/${key}`;
  }

  // ------------------------------------------------------------------
  // Backend I/O helpers
  // ------------------------------------------------------------------
  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    const text = await res.text();
    // Bug fix: this used to be a bare `return res.json()`, which throws
    // a raw JSON.parse SyntaxError - "Unexpected token 'I', "Internal
    // S"... is not valid JSON" was the exact message reported live - for
    // *any* non-JSON response body, most commonly the backend's own
    // default plain-text "Internal Server Error" page for an unhandled
    // exception. Every caller here already does `if (data.error) ...`,
    // so parsing defensively and reporting a real (if generic) .error
    // message for a non-JSON body lets that existing handling show
    // something a person can act on, instead of runWorkflow() (or
    // anything else that calls this) surfacing a bare parser error with
    // no indication of what the server actually said.
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch (e) {
      return { error: `${url} returned a non-JSON response (HTTP ${res.status}): ${text.slice(0, 300)}` };
    }
  }

  function pushConfig(node, key, value) {
    if (!node.properties) node.properties = {};
    node.properties[key] = value;
    if (node.psfId) {
      fetch(`/nodes/${node.psfId}/config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ config: { [key]: value } }),
      }).catch(() => {});
    }
    // Any config key change can affect the auto-derived attribute-slot
    // set (see syncAttributeSlots()/attributeEligibleKeys()) - not just
    // the legacy 'attributes' list field - e.g. an array widget's value
    // changing shape, or (via the Advanced JSON modal, which calls this
    // indirectly too) a key being retyped from an object to a scalar.
    // syncAttributeSlots() no-ops cheaply when the desired set hasn't
    // actually changed, so this is safe to call unconditionally.
    syncAttributeSlots(node);
    if (selectedNode === node) refreshDetailsPanel();
  }

  function nodeAction(psfId, action) {
    fetch(`/nodes/${psfId}/${action}`, { method: 'POST' }).then(() => {
      if (selectedNode && selectedNode.psfId === psfId) refreshDetailsPanel();
    }).catch(() => {});
  }

  // The manual-emit title-bar button (PSFNode.prototype.onMouseDown
  // below) and its right-click-menu twin both call this - unlike plain
  // nodeAction(), it gives visual feedback (a blink, matching the same
  // one live data triggers, plus a toast naming what got replayed),
  // since a title-bar icon click has no menu/panel of its own to confirm
  // anything happened.
  function manualEmitNode(node) {
    if (!node || !node.psfId) return;
    fetch(`/nodes/${node.psfId}/emit`, { method: 'POST' })
      .then((r) => r.json())
      .then((j) => {
        if (j && j.error) { toast(j.error); return; }
        nodeBlinkUntil[node.psfId] = Date.now() + 1200;
        if (graphcanvas) graphcanvas.setDirty(true, true);
        const ports = (j && j.replayed) ? Object.keys(j.replayed) : [];
        toast(ports.length
          ? `Manually emitted: ${ports.join(', ')}`
          : 'Nothing to emit yet - this node has no prior output');
        if (selectedNode === node) refreshDetailsPanel();
      })
      .catch(() => toast('Manual emit failed'));
  }

  // ------------------------------------------------------------------
  // Per-field widget kind (mirrors the old fieldWidgetHtml() decision)
  // ------------------------------------------------------------------
  function fieldOverride(psfType, key) {
    const forType = configSchema[psfType] || {};
    return forType[key] || forType[key.toLowerCase()];
  }

  function otherNodesValues(excludeNode) {
    const values = { '': '— none —' };
    (graph ? graph._nodes : []).forEach((n) => {
      if (n === excludeNode || !n.psfId) return;
      values[n.psfId] = `${n.title || n.psfType} (${n.psfId})`;
    });
    return values;
  }

  // Word-wraps `text` to fit within `maxWidth` pixels under the canvas
  // context's *currently set* font (the caller must set ctx.font before
  // calling this) - used by DisplayNode's live-view panel above to turn
  // an arbitrary (often long, often JSON-shaped) item into several lines
  // instead of clipping it to one. Falls back to a hard character-level
  // break for a single "word" that's wider than maxWidth all by itself
  // (e.g. a long unbroken JSON blob with no spaces), so a pathological
  // input can't overflow the box or loop forever.
  //
  // Feedback: "it is not respecting \n newline markers of the input and
  // just display one long line." Root cause: this used to split the
  // *whole* entry on /\s+/ in one pass - and \s matches a real newline
  // character just like a space, so an item containing actual embedded
  // "\n"s (e.g. multi-paragraph LLM output on LMStudioNode's `results`/
  // `reasoning` ports - see section 31) had every one of its line breaks
  // silently treated as a word-separating space and reflowed into one
  // continuous run of prose, only breaking again wherever the pixel
  // width happened to force a wrap - not at the input's own line breaks.
  // Fixed by splitting on real newlines *first*, into paragraphs, and
  // word-wrapping each paragraph independently - an explicit "\n" (or
  // "\r\n"/"\r") in the input now always starts a new line in the panel
  // regardless of how much room is left on the current one, and a blank
  // line (two newlines in a row) is preserved as a genuinely blank
  // output line rather than being collapsed away.
  function wrapText(ctx, text, maxWidth) {
    const lines = [];
    const paragraphs = String(text).split(/\r\n|\r|\n/);
    for (const paragraph of paragraphs) {
      const words = paragraph.split(/[ \t]+/).filter(Boolean);
      if (!words.length) {
        lines.push('');
        continue;
      }
      let cur = '';
      for (let word of words) {
        while (ctx.measureText(word).width > maxWidth) {
          let i = 1;
          while (i < word.length && ctx.measureText(word.slice(0, i + 1)).width <= maxWidth) i++;
          if (cur) { lines.push(cur); cur = ''; }
          lines.push(word.slice(0, i));
          word = word.slice(i);
        }
        const candidate = cur ? `${cur} ${word}` : word;
        if (cur && ctx.measureText(candidate).width > maxWidth) {
          lines.push(cur);
          cur = word;
        } else {
          cur = candidate;
        }
      }
      if (cur) lines.push(cur);
    }
    return lines.length ? lines : [''];
  }

  // Feedback: "i am unable to select text in the display node output. i
  // would like to do that to copy parts or highlight them." - used by
  // DisplayNode's '📋 copy visible' button (see addWidgetsForNode()) since
  // the live view is drawn straight onto the <canvas> and has no real DOM
  // text a browser could ever let the user click-drag select. This is the
  // one part of that gap a button CAN close: get the currently-visible
  // text onto the clipboard in one click. navigator.clipboard requires a
  // secure context (https, or localhost) - falls back to the classic
  // hidden-textarea + execCommand('copy') trick for anything else (e.g.
  // the UI served over plain http on a LAN address).
  function copyTextToClipboard(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).catch(() => copyTextToClipboardFallback(text));
    } else {
      copyTextToClipboardFallback(text);
    }
  }

  function copyTextToClipboardFallback(text) {
    try {
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.focus();
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    } catch (e) { /* nothing more we can do - clipboard access just isn't available here */ }
  }

  function addWidgetsForNode(node) {
    node.widgets = [];
    // Feature request: "add a really nice visually looking realtime view
    // of the last item received by a display node into its own canvas as
    // a text field which is reasonably sized and above the attributes
    // etc." - added FIRST, before any config widget below, so it's the
    // topmost thing drawn on the node body. This vendored litegraph build
    // has no built-in 'custom' widget type, but drawNodeWidgets() falls
    // back to calling a widget's own `.draw(ctx, node, width, y, height)`
    // for any unrecognized `type` (see vendor/litegraph.core.min.js) -
    // exactly the same mechanism litegraph.js's real 'custom' widgets use
    // upstream - so this hand-drawn panel is a real widget as far as
    // sizing/layout is concerned without needing any change to the
    // vendored library. It has no `.mouse` handler, so clicks on it are
    // simply no-ops (see the same fallback's `default: r.mouse&&(...)`
    // click-handling branch) - read-only by design, exactly like the
    // small disabled text widgets used elsewhere for other read-only
    // fields.
    //
    // Feature request: "let the live view be a multi line text view and
    // make it scale with the node size" - this used to be a fixed
    // DISPLAY_LIVE_VIEW_HEIGHT-tall box showing a single un-wrapped line
    // (the single latest item, clipped rather than wrapped if it didn't
    // fit) with 5 separate small single-line widgets stacked below it for
    // scrollback - dragging the node taller just left dead space below
    // the stack; the two mechanisms were also redundant with each other.
    // Replaced with one panel that word-wraps its content across however
    // many lines fit, showing DISPLAY_HISTORY_LINES' worth of scrollback
    // (see n._liveViewEntries, populated by pollNodeStatus() below)
    // instead of only the single latest item, and whose own computeSize()
    // grows by exactly however much taller than its natural default size
    // the node has been manually resized to - see
    // node._displayLiveViewBaseHeight below (set once, right after this
    // node's default node.size is first computed) for how that's derived
    // without needing to know litegraph's own title-bar/slot-row pixel
    // constants.
    if (node.psfType === 'DisplayNode') {
      const liveWidget = node.addWidget('psf_live_view', 'live', '', () => {});
      liveWidget.computeSize = function (w) {
        const base = node._displayLiveViewBaseHeight;
        const h = (base != null && node.size)
          ? DISPLAY_LIVE_VIEW_MIN_HEIGHT + Math.max(0, node.size[1] - base)
          : DISPLAY_LIVE_VIEW_MIN_HEIGHT;
        return [w, h];
      };
      liveWidget.draw = function (ctx, n, widgetWidth, y) {
        const pad = 8;
        const boxH = liveWidget.computeSize(widgetWidth)[1] - 8;
        ctx.save();
        const grad = ctx.createLinearGradient(0, y, 0, y + boxH);
        grad.addColorStop(0, '#0f172a');
        grad.addColorStop(1, '#111827');
        ctx.fillStyle = grad;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(pad, y, widgetWidth - pad * 2, boxH, [8]);
        else ctx.rect(pad, y, widgetWidth - pad * 2, boxH);
        ctx.fill();
        ctx.strokeStyle = 'rgba(34,197,94,0.4)';
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.fillStyle = '#94a3b8';
        ctx.font = '9px -apple-system, "Segoe UI", sans-serif';
        ctx.textAlign = 'left';
        ctx.fillText('● LIVE', pad + 8, y + 13);
        const entries = n._liveViewEntries || [];
        // Feedback: "displayed text in a display node is not scrollable" -
        // this panel only ever drew from the newest entry downward and
        // silently clipped anything past whatever height happened to fit
        // (see the DISPLAY_HISTORY_LINES comment above - up to 40 entries
        // are fetched, almost always far more than the default ~70px-tall
        // panel can show), with no way to reach the rest short of dragging
        // the whole node taller. n._liveViewScroll (advanced by the
        // '▲ older'/'▼ newer' buttons added in addWidgetsForNode() - see
        // their own comment) slices however many of the newest entries to
        // skip before drawing below. Scroll is in whole entries, not
        // wrapped lines - simpler, and the panel already scrolls one full
        // logged message per click either way.
        //
        // This used to be wheel-driven (hovering the panel and scrolling)
        // via a handleDisplayLiveViewWheel() hooked directly on the canvas
        // element, ahead of litegraph's own wheel/zoom handler. Feedback:
        // "during testing i resize the display node to a big output size.
        // now the mousewheel zooming is not working anymore." - a
        // DisplayNode resized big makes this panel cover most of the
        // node, so almost any wheel event near it landed inside the
        // panel's rectangle and got captured as "scroll the log" instead
        // of "zoom the graph", often with no visible effect (little or no
        // scrollback to move through) - it just looked like zoom had
        // silently broken. Buttons sidestep that entirely: a plain wheel
        // now always zooms the graph, everywhere, no matter how big the
        // node is, and scrolling the panel is only ever a deliberate
        // click. Clamped here too (not just in the button handlers) since
        // the entry list can shrink out from under a stale scroll
        // position between poll ticks (e.g. after a node reset).
        const scroll = Math.min(n._liveViewScroll || 0, Math.max(0, entries.length - 1));
        n._liveViewScroll = scroll;
        const visible = scroll > 0 ? entries.slice(scroll) : entries;
        if (entries.length > 1) {
          ctx.save();
          ctx.fillStyle = 'rgba(148,163,184,0.75)';
          ctx.font = '8px -apple-system, "Segoe UI", sans-serif';
          ctx.textAlign = 'right';
          ctx.fillText(
            scroll > 0 ? `↕ ${scroll + 1}/${entries.length} · ▲▼ to scroll` : '▲▼ buttons scroll history',
            pad + (widgetWidth - pad * 2) - 8, y + 13,
          );
          ctx.restore();
        }
        ctx.save();
        const clipX = pad + 6, clipY = y + 18;
        const clipW = widgetWidth - pad * 2 - 12, clipH = boxH - 22;
        ctx.beginPath();
        ctx.rect(clipX, clipY, clipW, clipH);
        ctx.clip();
        ctx.fillStyle = '#4ade80';
        ctx.font = '11px "Courier New", monospace';
        ctx.textAlign = 'left';
        const lineHeight = 13;
        const maxTextWidth = clipW - 6;
        if (!visible.length) {
          ctx.fillText('(no data yet)', clipX + 6, clipY + 12);
        } else {
          let ly = clipY + 12;
          const bottom = clipY + clipH + lineHeight; // one extra line's slack, then clipped anyway
          outer:
          for (const entry of visible) {
            for (const line of wrapText(ctx, entry, maxTextWidth)) {
              if (ly > bottom) break outer;
              ctx.fillText(line, clipX + 6, ly);
              ly += lineHeight;
            }
          }
        }
        ctx.restore();
        ctx.restore();
      };
      node._liveViewWidget = liveWidget;
      node._liveViewScroll = 0;
    }
    const cfg = node.properties || {};
    Object.keys(cfg).forEach((key) => {
      const value = cfg[key];
      const override = fieldOverride(node.psfType, key);
      if (override && override.kind === 'node_ref') {
        const values = otherNodesValues(node);
        if (value && !(value in values)) values[value] = `${value} (not on canvas)`;
        node.addWidget('combo', key, value == null ? '' : String(value), (v) => pushConfig(node, key, v), { values });
      } else if (override && override.kind === 'enum') {
        const opts = override.options || [];
        const sel = opts.includes(value) ? value : opts[0];
        node.addWidget('combo', key, sel, (v) => pushConfig(node, key, v), { values: opts });
      } else if (typeof value === 'boolean') {
        node.addWidget('toggle', key, value, (v) => pushConfig(node, key, v));
      } else if (typeof value === 'number') {
        node.addWidget('number', key, value, (v) => pushConfig(node, key, v));
      } else if (Array.isArray(value)) {
        node.addWidget('text', key, value.join(','), (v) => pushConfig(node, key, v.split(',').map((s) => s.trim()).filter((s) => s.length)));
      } else if (value && typeof value === 'object') {
        const w = node.addWidget('text', key, JSON.stringify(value), () => {});
        w.disabled = true;
      } else {
        node.addWidget('text', key, value == null ? '' : String(value), (v) => pushConfig(node, key, v));
      }
    });
    // DisplayNode's whole job is to show a human whatever flows through
    // it, but until now the only way to actually see that was to select
    // it and open the side-panel live view - the node itself gave no
    // visual feedback at all (unlike, say, ClockNode's own value or
    // ListStringsNode's progress bar). This used to be a real, inline
    // "last value" readout plus a short scrollback here as N separate
    // small single-line widgets, alongside the live-view panel added
    // above - both fed the same underlying scrollback and neither wrapped
    // long text, just clipped it. Superseded by that live-view panel
    // itself becoming a multi-line, node-size-scaling view (see
    // addWidgetsForNode()'s psf_live_view block above and
    // pollNodeStatus()'s n._liveViewEntries below) - keeping both would
    // just show the same scrollback twice.
    node.addWidget('button', '+ field / advanced JSON…', null, () => openAdvancedModal(node));
    if (node.psfPairedInOut) {
      // SyncBarrierNode/LedActivityNode: a single "+ port"/"− port" pair
      // grows or shrinks a matched inN/outN pair together (see
      // rebuildPorts()'s psfPairedInOut comment for why these two node
      // types can't use the normal independent "+ input"/"+ output"
      // controls below without risking the two counts drifting apart).
      node.addWidget('button', '+ port', null, () => {
        const n = (node.inputs || []).filter((i) => !i.psfKind).length;
        if (n >= DYNAMIC_SLOT_MAX) return;
        node.addInput('in' + n, 0);
        node.addOutput('out' + n, 0);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      node.addWidget('button', '− port', null, () => {
        const plain = (node.inputs || []).filter((i) => !i.psfKind);
        if (plain.length <= DYNAMIC_SLOT_MIN) return;
        const last = plain.length - 1;
        const inIdx = (node.inputs || []).findIndex((i) => i.name === 'in' + last);
        if (inIdx >= 0) node.removeInput(inIdx);
        const outIdx = (node.outputs || []).findIndex((o) => o.name === 'out' + last);
        if (outIdx >= 0) node.removeOutput(outIdx);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    } else if (node.psfDynamicInputs) {
      node.addWidget('button', '+ input', null, () => {
        if ((node.inputs || []).filter((i) => !i.psfKind).length >= DYNAMIC_SLOT_MAX) return;
        const n = (node.inputs || []).filter((i) => !i.psfKind).length;
        node.addInput('in' + n, 0);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      // Bug fix: there used to be no way to shrink a dynamic-port node
      // back down once you'd clicked "+ input" (or it had grown past the
      // 4 slots it starts with) - the only options were to leave the
      // extra, always-empty slots on the canvas forever or delete and
      // recreate the whole node. Removes the highest-numbered plain
      // (non-attribute/non-control) input slot, same as "+ input" always
      // adds the next one in sequence; disconnects cleanly via
      // LiteGraph's own removeInput (which detaches any link on that slot
      // first). Never removes below DYNAMIC_SLOT_MIN, and never touches
      // the reserved attribute/control slots - those aren't part of this
      // "how many data ports does this node have" count at all.
      node.addWidget('button', '− input', null, () => {
        const plain = (node.inputs || [])
          .map((inp, idx) => ({ inp, idx }))
          .filter((e) => !e.inp.psfKind);
        if (plain.length <= DYNAMIC_SLOT_MIN) return;
        const last = plain[plain.length - 1];
        node.removeInput(last.idx);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    }
    if (node.psfPairedInOut) {
      // Already handled above, as a single "+ port"/"− port" pair
      // alongside the input-side controls - no separate output-side
      // buttons for these two node types.
    } else if (node.psfDynamicOutputs && node.psfPairedRawOutputs) {
      // ForkNode: "+ output"/"− output" grow or shrink an out{n}/raw{n}
      // pair together, so every normal duplicated-stream output always
      // has its raw counterpart (see rebuildPorts()'s comment on
      // psfPairedRawOutputs) - counting only the 'outN' half keeps the
      // pair count and the DYNAMIC_SLOT_MIN/MAX range in terms of "how
      // many normal outputs", matching how the non-paired case above
      // counts its outputs.
      node.addWidget('button', '+ output', null, () => {
        const n = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name)).length;
        if (n >= DYNAMIC_SLOT_MAX) return;
        node.addOutput('out' + n, 0);
        node.addOutput('raw' + n, 0, { color_on: RAW_PORT_COLOR, color_off: RAW_PORT_COLOR, label: '⇢ raw' + n });
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      node.addWidget('button', '− output', null, () => {
        const n = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name)).length;
        if (n <= DYNAMIC_SLOT_MIN) return;
        // Remove by name (not by trailing index) since the two halves of
        // the highest-numbered pair aren't guaranteed to be the two
        // highest-index slots in node.outputs - a user can reconnect/
        // reorder slots in LiteGraph without changing their names.
        const last = n - 1;
        ['out' + last, 'raw' + last].forEach((name) => {
          const idx = (node.outputs || []).findIndex((o) => o.name === name);
          if (idx >= 0) node.removeOutput(idx);
        });
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    } else if (node.psfSplitByValue) {
      // "make value->output attributes expandable in number, add output
      // accordingly" - a single button grows the `values` config array
      // AND its matching out{n} port together (excluding the always-
      // present, unpaired 'default' output from the count), so the
      // value->output mapping is never a hidden index a person has to
      // keep in sync by hand - editing the resulting comma-separated
      // `values` text widget then sets what each numbered port actually
      // matches.
      node.addWidget('button', '+ value / output', null, () => {
        const numbered = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name));
        const n = numbered.length;
        if (n >= DYNAMIC_SLOT_MAX) return;
        const values = Array.isArray(node.properties.values) ? node.properties.values.slice() : [];
        values.push(`value${n + 1}`);
        pushConfig(node, 'values', values);
        node.addOutput('out' + n, 0);
        node.setSize(node.computeSize());
        // Rebuilds every widget (including the 'values' text widget
        // itself) from the now-updated node.properties, so the visible
        // comma-separated list immediately reflects the new entry instead
        // of only updating internally until something else happens to
        // trigger a redraw.
        addWidgetsForNode(node);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      node.addWidget('button', '− value / output', null, () => {
        const numbered = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name));
        const n = numbered.length;
        if (n <= DYNAMIC_SLOT_MIN) return;
        const values = Array.isArray(node.properties.values) ? node.properties.values.slice() : [];
        values.pop();
        pushConfig(node, 'values', values);
        const idx = (node.outputs || []).findIndex((o) => o.name === 'out' + (n - 1));
        if (idx >= 0) node.removeOutput(idx);
        node.setSize(node.computeSize());
        addWidgetsForNode(node);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    } else if (node.psfRoundRobin) {
      // "cycles through all outputs" needs an authoritative count the
      // backend node can actually read (self.config['count'] - see
      // round_robin_node.py's init()), not just however many ports happen
      // to be drawn on the canvas - so, like SplitByValueNode's `values`
      // above, these buttons push `count` and add/remove the matching
      // out{n} port together rather than letting the two drift apart.
      node.addWidget('button', '+ output', null, () => {
        const n = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name)).length;
        if (n >= DYNAMIC_SLOT_MAX) return;
        pushConfig(node, 'count', n + 1);
        node.addOutput('out' + n, 0);
        node.setSize(node.computeSize());
        addWidgetsForNode(node);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      node.addWidget('button', '− output', null, () => {
        const n = (node.outputs || []).filter((o) => /^out\d+$/.test(o.name)).length;
        if (n <= DYNAMIC_SLOT_MIN) return;
        pushConfig(node, 'count', n - 1);
        const idx = (node.outputs || []).findIndex((o) => o.name === 'out' + (n - 1));
        if (idx >= 0) node.removeOutput(idx);
        node.setSize(node.computeSize());
        addWidgetsForNode(node);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    } else if (node.psfDynamicOutputs) {
      node.addWidget('button', '+ output', null, () => {
        const n = (node.outputs || []).length;
        if (n >= DYNAMIC_SLOT_MAX) return;
        node.addOutput('out' + n, 0);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      // Mirrors "− input" above, for dynamic output ports (e.g. SubgraphNode).
      node.addWidget('button', '− output', null, () => {
        const n = (node.outputs || []).length;
        if (n <= DYNAMIC_SLOT_MIN) return;
        node.removeOutput(n - 1);
        node.setSize(node.computeSize());
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
    }
    if (node.psfType === 'DisplayNode') {
      // Feedback: "during testing i resize the display node to a big
      // output size. now the mousewheel zooming is not working anymore.
      // the buttons still work." - see the long comment inside the
      // psf_live_view widget's draw() above for the full story: wheel-
      // driven scrolling of the live view is gone, replaced by these two
      // explicit buttons (same pattern as '+ input'/'− input' etc. on
      // other node types), so a plain wheel always zooms the graph now,
      // regardless of how big this node has been resized.
      node.addWidget('button', '▲ older', null, () => {
        const entries = node._liveViewEntries || [];
        const maxScroll = Math.max(0, entries.length - 1);
        node._liveViewScroll = Math.min(maxScroll, (node._liveViewScroll || 0) + 1);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      node.addWidget('button', '▼ newer', null, () => {
        node._liveViewScroll = Math.max(0, (node._liveViewScroll || 0) - 1);
        if (graphcanvas) graphcanvas.setDirty(true, true);
      });
      // Feedback: "i am unable to select text in the display node output.
      // i would like to do that to copy parts or highlight them." - the
      // live view is drawn straight onto the <canvas>, which has no real
      // text for a browser to select at all (pixels, not DOM nodes), so
      // genuine click-drag selection there would need a whole custom
      // per-character hit-testing/highlighting system. This button is the
      // pragmatic middle ground: one click copies whatever's currently
      // visible in the panel (same scroll position as the buttons above)
      // to the clipboard, so the user can paste it into a real text
      // field/editor to select or highlight parts of it there.
      node.addWidget('button', '📋 copy visible', null, () => {
        const entries = node._liveViewEntries || [];
        const scroll = Math.min(node._liveViewScroll || 0, Math.max(0, entries.length - 1));
        const visible = scroll > 0 ? entries.slice(scroll) : entries;
        copyTextToClipboard(visible.join('\n'));
      });
    }
    node.size = node.computeSize();
    // Freezes "this node's natural total height, with the live view at
    // its own minimum" as a baseline the live view's own computeSize()
    // (added above) reads back to work out how much extra height a
    // manual resize has given the node, without needing to know
    // litegraph's own title-bar/slot-row pixel constants at all: at the
    // moment this line runs, node._displayLiveViewBaseHeight is still
    // unset, so the live view's computeSize() just returned
    // DISPLAY_LIVE_VIEW_MIN_HEIGHT into the computeSize() call above -
    // exactly the "everything else at its natural size, live view at its
    // minimum" total this is meant to capture. Recomputed every time this
    // function runs (node creation, and any later full widget rebuild),
    // so it stays correct if the node's other widgets ever change shape.
    if (node.psfType === 'DisplayNode') node._displayLiveViewBaseHeight = node.size[1];
  }

  // ------------------------------------------------------------------
  // Attribute-target input slots: kept in sync with config.attributes
  // (see core/node.py BaseNode.set_attribute()/Engine._pump_attribute()).
  // These are drop TARGETS only - a value wired here from any other
  // node's output maps directly onto this node's own config attribute at
  // runtime, rather than being read via self.inputs.get(portname) like a
  // normal port.
  // ------------------------------------------------------------------
  function attributeEligibleKeys(node) {
    // Every rendered config widget's key is eligible for its own
    // attribute-wire edge point automatically - see addWidgetsForNode()
    // above, which renders one widget per node.properties key. The only
    // keys excluded are 'attributes' itself (the old manual opt-in list,
    // kept below for backward compatibility with configs that still set
    // it) and plain-object-valued keys, which addWidgetsForNode() renders
    // as a *disabled* read-only JSON text widget rather than something a
    // user actually edits here - wiring a live value onto a field nothing
    // lets you edit directly would be confusing, not useful.
    const cfg = node.properties || {};
    return Object.keys(cfg).filter((k) => {
      if (k === 'attributes') return false;
      const v = cfg[k];
      return !(v && typeof v === 'object' && !Array.isArray(v));
    });
  }

  function syncAttributeSlots(node) {
    // Bug fix (the concrete "if a script node had an input field defined
    // it should have an edge point" request): attribute-target slots used
    // to be created *only* for names an author had manually typed into a
    // node's `attributes` array field - itself only reachable through the
    // Advanced JSON modal, with no discoverable per-widget affordance at
    // all. Every config widget a node actually renders (see
    // attributeEligibleKeys() above) now automatically gets one too, so
    // e.g. ScriptNode's `script` field has a labeled edge point without
    // anyone having to know the `attributes` array trick exists. The
    // manual list still works and is honored first (so an explicit
    // `attributes` entry keeps its position/precedence) for any name that
    // isn't already implied by an existing widget - e.g. a name a plugin
    // or hand-edited workflow YAML wants wired even though this node
    // build doesn't render a widget for it.
    const manual = Array.isArray(node.properties && node.properties.attributes)
      ? node.properties.attributes.map(String) : [];
    const auto = attributeEligibleKeys(node);
    const desired = manual.concat(auto.filter((k) => !manual.includes(k)));
    const current = (node.inputs || []).filter((i) => i.psfKind === 'attribute').map((i) => i.name);
    if (JSON.stringify(current) === JSON.stringify(desired)) return;
    for (let i = (node.inputs || []).length - 1; i >= 0; i--) {
      if (node.inputs[i].psfKind === 'attribute') node.removeInput(i);
    }
    desired.forEach((name) => {
      // `label` is what LiteGraph actually draws next to the slot (`name`
      // stays the real port name edges/serialization use) - prefixed so
      // an attribute-target slot reads unambiguously as "wire a value
      // onto this specific field" rather than looking like a normal data
      // port, satisfying the "at least a hover tooltip" ask with
      // something more visible: an always-on label naming the exact
      // attribute a wire dropped here will target.
      node.addInput(name, 0, { psfKind: 'attribute', color_on: '#a855f7', color_off: '#a855f7', label: '◆ ' + name });
    });
    node.setSize(node.computeSize());
    // Bug fix: DisplayNode's live-view panel (see addWidgetsForNode()'s
    // psf_live_view block) sizes itself off node._displayLiveViewBaseHeight
    // - "this node's natural total height with the live view at its own
    // minimum" - captured once in addWidgetsForNode(), *before* this
    // function ever runs. Since onNodeCreated() always calls this right
    // after addWidgetsForNode(), and 'prefix'/'suffix' are both
    // auto-eligible for an attribute slot (see attributeEligibleKeys()),
    // the very first real syncAttributeSlots() call adds slots that
    // addWidgetsForNode()'s own base capture never saw - inflating
    // node.computeSize() by however much those slots' rows add, which the
    // live view's own computeSize() would otherwise misread as "the user
    // manually resized this node" and add on top of its own minimum
    // height, even though nothing was ever dragged. Re-capturing here,
    // whenever this function actually changes the slot set (not on the
    // common early-return-above no-op path), keeps the baseline correct
    // through the node's whole life, including a later attribute-slot
    // change from the Advanced JSON modal.
    if (node.psfType === 'DisplayNode') node._displayLiveViewBaseHeight = node.size[1];
    if (graphcanvas) graphcanvas.setDirty(true, true);
  }

  function findInputSlotIndex(node, name) {
    let idx = (node.inputs || []).findIndex((i) => i.name === name && i.psfKind !== 'attribute');
    if (idx < 0 && node.psfDynamicInputs) {
      node.addInput(name, 0);
      idx = node.inputs.length - 1;
    }
    return idx;
  }
  function findOutputSlotIndex(node, name) {
    let idx = (node.outputs || []).findIndex((o) => o.name === name);
    if (idx < 0 && node.psfDynamicOutputs) {
      node.addOutput(name, 0);
      idx = node.outputs.length - 1;
    }
    return idx;
  }
  function ensureAttributeSlot(node, name) {
    let idx = (node.inputs || []).findIndex((i) => i.name === name && i.psfKind === 'attribute');
    if (idx < 0) {
      const attrs = Array.isArray(node.properties.attributes) ? node.properties.attributes.slice() : [];
      if (!attrs.includes(name)) { attrs.push(name); node.properties.attributes = attrs; }
      syncAttributeSlots(node);
      idx = (node.inputs || []).findIndex((i) => i.name === name && i.psfKind === 'attribute');
    }
    return idx;
  }

  // ------------------------------------------------------------------
  // Port setup from /node-schema (core/port_schema.py)
  // ------------------------------------------------------------------
  function rebuildPorts(node) {
    const schema = portSchema[node.psfType] || { inputs: ['in'], outputs: ['out'] };
    const isDynIn = schema.inputs === '*';
    const isDynOut = schema.outputs === '*';
    node.psfDynamicInputs = isDynIn;
    node.psfDynamicOutputs = isDynOut;
    // ForkNode is the one DYNAMIC-output type whose normal outputs each
    // need a paired "raw" counterpart (explicit ask: "fork needs multiple
    // raw outputs, each for every normal output" - unlike SubgraphNode,
    // whose DYNAMIC outputs are config-driven bridge names, not this
    // out0/out1/... numbered scheme, so it's excluded here). When this is
    // set, the numbered dynamic-output slots below are built as
    // out0/raw0, out1/raw1, ... pairs instead of a plain out0..outN list,
    // and the "+ output"/"− output" buttons in addWidgetsForNode() grow
    // or shrink both halves of a pair together.
    node.psfPairedRawOutputs = isDynOut && node.psfType === 'ForkNode' && SHOW_RAW_PORTS;
    // SyncBarrierNode/LedActivityNode: DYNAMIC on *both* sides, and their
    // whole point requires the two counts never drifting apart (a barrier
    // needs one output per input to release onto; the LED relay needs a
    // same-index output for every input it forwards) - see
    // addWidgetsForNode()'s matching "+ port"/"− port" branch below, which
    // grows or shrinks an inN/outN pair together for these two node types
    // instead of the normal independent "+ input"/"+ output" controls.
    node.psfPairedInOut = isDynIn && isDynOut && (node.psfType === 'SyncBarrierNode' || node.psfType === 'LedActivityNode');
    // SplitByValueNode: DYNAMIC output only, but each numbered output
    // port (out0, out1, ...) corresponds 1:1 with an entry in this node's
    // own `values` config array, plus one always-present, unpaired
    // 'default' output for anything that matches none of them - see
    // addWidgetsForNode()'s dedicated "+ value/output" branch below.
    node.psfSplitByValue = node.psfType === 'SplitByValueNode';
    // RoundRobinNode: DYNAMIC output only, but each numbered output port
    // (out0, out1, ...) corresponds 1:1 with this node's own `count`
    // config (how many turns the cycle has) - no per-slot value to keep
    // in sync like SplitByValueNode's `values`, just a single number, so
    // its own "+ output"/"− output" branch below just increments/
    // decrements `count` alongside the port count.
    node.psfRoundRobin = node.psfType === 'RoundRobinNode';
    const ins = isDynIn ? Array.from({ length: DYNAMIC_SLOT_COUNT }, (_, i) => 'in' + i) : (schema.inputs || []);
    let outs;
    if (node.psfPairedRawOutputs) {
      outs = [];
      for (let i = 0; i < DYNAMIC_SLOT_COUNT; i++) { outs.push('out' + i, 'raw' + i); }
    } else if (node.psfSplitByValue) {
      // Sized from the node's own `values` config (already populated by
      // the time this runs - see definePsfNodeClass()'s constructor,
      // which sets node.properties before calling rebuildPorts()) rather
      // than the generic DYNAMIC_SLOT_COUNT default, so a freshly created
      // node's port count always starts matched to its values list; plus
      // one always-present, unpaired 'default' output.
      const valueCount = Array.isArray(node.properties && node.properties.values)
        ? node.properties.values.length : DYNAMIC_SLOT_COUNT;
      outs = Array.from({ length: valueCount }, (_, i) => 'out' + i);
      outs.push('default');
    } else if (node.psfRoundRobin) {
      // Sized from the node's own `count` config (already populated by
      // the time this runs - see definePsfNodeClass()'s constructor,
      // which sets node.properties before calling rebuildPorts()) rather
      // than the generic DYNAMIC_SLOT_COUNT default, so a freshly created
      // node's port count always starts matched to its real cycle length.
      const cycleCount = Number.isFinite(node.properties && node.properties.count)
        ? node.properties.count : DYNAMIC_SLOT_COUNT;
      outs = Array.from({ length: cycleCount }, (_, i) => 'out' + i);
    } else {
      outs = isDynOut ? Array.from({ length: DYNAMIC_SLOT_COUNT }, (_, i) => 'out' + i) : (schema.outputs || []);
    }
    // No further filtering needed here any more: Fork's own psfPairedRawOutputs
    // branch above already doesn't add any rawN names to `outs` while
    // SHOW_RAW_PORTS is false (the only remaining source of one), and for
    // every other node type `outs` now comes straight from the backend's
    // real, current port schema (Phase 2 deleted the generic server-side
    // auto-duplication that used to put a raw/raw_<name> entry in there).
    // A blanket post-filter here used to also strip ApiOutputNode's own
    // real, hand-declared 'raw' port - a genuine port name, not an
    // auto-duplicate - making it permanently unwireable from the editor;
    // that bug is what removing this filter fixes.
    ins.forEach((name) => node.addInput(name, 0));
    const isTrigger = node.constructor && node.constructor.psfIsTrigger;
    outs.forEach((name) => {
      let extra;
      if (RAW_PORT_RE.test(name)) {
        // Same-data, unencapsulated tap point (ApiOutputNode's fixed
        // 'raw' port, or one of ForkNode's paired 'rawN' ports) - styled
        // distinctly so it doesn't look like just another numbered data
        // slot; label makes the "unencapsulated" meaning explicit on
        // hover/at a glance, matching the '◆ '/'⚡ ' label convention
        // already used for attribute/control slots above.
        extra = { color_on: RAW_PORT_COLOR, color_off: RAW_PORT_COLOR, label: '⇢ ' + name };
      } else if (isTrigger) {
        extra = { color_on: KIND_COLORS.control, color_off: KIND_COLORS.control };
      }
      node.addOutput(name, 0, extra);
    });
    // Every node - even a pure source/emitter with zero declared data
    // input ports (LogInputNode, GeneratorInputNode, MQTTInputNode,
    // TimerNode, ...) - gets one dedicated, always-present slot for
    // receiving a trigger/control wire, separate from its data ports.
    // Before this, the *only* way to wire a trigger onto a node was to
    // drop it onto one of the node's ordinary data input slots (which a
    // zero-input node simply didn't have at all), and doing so on a node
    // that did have a data port silently reused that port's real pipe for
    // the control wire too - see Engine._wire_edges()'s matching fix and
    // its comment for the exact corruption this caused. This slot's name
    // is always literally 'control', which BaseNode.add_input() already
    // special-cases (see core/node.py) to route into a node's separate
    // control-message pipe rather than its normal self.inputs dict, so it
    // can never collide with a real data port no matter what the target
    // node type's own port schema says.
    node.addInput('control', 0, {
      psfKind: 'control',
      label: '⚡ control',
      color_on: KIND_COLORS.control,
      color_off: KIND_COLORS.control,
    });
  }

  // ------------------------------------------------------------------
  // Dynamic LiteGraph node class per pystreamflow node type
  // ------------------------------------------------------------------

  // Real default config for every node type, keyed by the backend class
  // name (meta.type) - not just cosmetic. Before this, every freshly
  // created node started with `properties = {}` (empty), and
  // addWidgetsForNode() only ever renders a widget for a key already
  // present in `properties` - so a brand new node showed NO config
  // fields at all until a person discovered the "+ field / advanced
  // JSON..." button and knew, unassisted, exactly which key name(s) the
  // node's own Python source expects (e.g. FileInputNode's path being
  // named `path`, not `filepath`). Worse than a UX gap for any node whose
  // init() raises if a required key is missing entirely (FileInputNode,
  // ScriptInputNode, UrlInputNode, ...): a freshly dropped one would run
  // and immediately error out server-side with no visible field showing
  // what needed to be filled in. These defaults are each taken from
  // actually reading the corresponding node's init() - not guessed - and
  // deliberately leave out a handful of keys that are either computed
  // per-node-id server-side (e.g. WebInputNode's `path` defaults to
  // `/in/{id}`; forcing a static default here would silently override
  // that with the same value for every node) or confirmed dead/unused by
  // the same audit (e.g. SocketInputNode's `proto`, WebOutputNode's
  // `host`/`port`/`format`) - showing a field that provably does nothing
  // would just relocate the misleading-config bug into the UI instead of
  // fixing it.
  const DEFAULT_CONFIG = {
    WebInputNode: { host: '127.0.0.1', port: 8080 },
    // Unlike WebInputNode's `path` (excluded above precisely so a static
    // default here can't clobber its per-node-id server-side default),
    // `uri` is deliberately shown as a visible '' field: it's the node's
    // one main user-facing attribute (the whole point of this node type
    // is "type a uri, get a POST route at /api/<uri>"), and both
    // ApiInputNode/ApiOutputNode fall back to the node's own id on an
    // empty string just as much as on a missing key, so seeding '' here
    // doesn't break that default the way a static `path` value would for
    // WebInputNode.
    // No host/port fields: unlike WebInputNode (an arbitrary user-chosen
    // path on the shared server), ApiInputNode/ApiOutputNode's routes are
    // always namespaced under the shared server's fixed /api prefix, so
    // exposing per-node host/port here would be misleading (there's only
    // ever one shared server, already started with its own host/port by
    // whichever node starts first) - matching WebOutputNode/
    // WebOutputJSONNode's precedent below, which also omit them. The
    // backend (nodes/input_api.py) still accepts host/port in config for
    // tests/advanced use (e.g. `port: 0` for an ephemeral test socket);
    // this only removes the UI widgets, since 'uri' is genuinely all a
    // user needs to configure day to day.
    ApiInputNode: { uri: '' },
    FileInputNode: { path: '', poll_interval: 1.0 },
    DirectoryInputNode: { path: '', recursive: false, poll_interval: 2.0, extensions: '', media_only: false, emit_as: 'path' },
    MediaFileInputNode: { path: '', emit_on: 'change', poll_interval: 1.0 },
    MediaFileOutputNode: { path_pattern: '', overwrite: false },
    LMStudioNode: { base_url: 'http://localhost:1234/v1', model: 'local-model', api_key: 'lm-studio', system: '', timeout: 600, image_prompt: 'Describe this image.' },
    ImageDecodeNode: { auto_orient: false },
    ImageResizeNode: { max_side: 1024, width: '', height: '', keep_aspect: true, resample: 'lanczos', upscale: false },
    ImageCropNode: { width: 512, height: 512, x: 0, y: 0, center: false },
    ImageRotateNode: { angle: 90, expand: true, fill: '#000000' },
    ImageFlipNode: { direction: 'horizontal' },
    ImageConvertNode: { format: 'jpeg', quality: 85, mode: 'keep' },
    ImageFilterNode: { filter: 'none', radius: 2, brightness: 1.0, contrast: 1.0, saturation: 1.0, sharpness: 1.0 },
    ImageInfoNode: { exif: true },
    ImageThumbnailNode: { max_side: 256, format: 'webp', quality: 80 },
    AudioDecodeNode: { chunk_ms: 0 },
    AudioEncodeNode: { format: 'wav', bitrate: '128k', segment_s: 0, flush_idle_s: 2.0 },
    AudioResampleNode: { sample_rate: 16000, channels: 1 },
    AudioGainNode: { gain_db: 0 },
    AudioNormalizeNode: { mode: 'peak', target_db: -1, max_gain_db: 30 },
    AudioLevelNode: { silence_db: -50 },
    AudioSegmentNode: { mode: 'silence', threshold_db: -40, min_silence_ms: 500, pad_ms: 200, min_segment_s: 0.3, max_segment_s: 30, segment_s: 10, flush_idle_s: 2.0 },
    SpeechToTextNode: { backend: 'api', base_url: 'http://localhost:8000/v1', model: 'whisper-1', api_key: '', language: '', prompt: '', timeout: 300 },
    ScriptInputNode: { script_path: '', interpreter: '', args: [], interval: 5.0, working_dir: '', timeout: 30.0, emit_mode: 'lines' },
    MQTTInputNode: { broker: 'localhost', port: 1883, topic: '#', client_id: '', username: '', password: '', qos: 0 },
    ConstantValueNode: { value: '', repeat: false, interval: 1.0 },
    // Feedback: "is there a delay in the generator node (which would be
    // ok, if editable as attribute)? it is quite slow." - nodes/
    // input_generator.py's `interval` config field (seconds between
    // emissions, default 0.5) has been fully live/configurable since it
    // was added - it just wasn't in this DEFAULT_CONFIG entry, so
    // addWidgetsForNode() (which only ever renders a widget for a key
    // already present in node.properties) never showed it, making the
    // backend's real, working knob invisible and looking unconfigurable
    // from the canvas. `value` is deliberately still left out: the
    // backend distinguishes "key absent" (auto-incrementing counter, the
    // default) from "key present" (even as '' - always emits that literal
    // value) via `self.config.get('value')` returning `None` only when
    // the key is missing entirely, so seeding a default here would
    // silently turn off the counter behavior for every new Generator node.
    GeneratorInputNode: { count: 10, interval: 0.5 },
    UrlInputNode: { url: '', method: 'GET', headers: {}, poll_interval: 5.0, timeout: 10.0 },
    SocketInputNode: { host: '0.0.0.0', port: 9001 },
    // 'sse' removed (feedback: "making no sense" as a canvas config
    // field - see user_input_node.py's class docstring) - both the SSE
    // stream and the plain poll endpoint are always available now, so
    // there's nothing left to toggle.
    UserInputNode: { prompt: 'Enter value:', allow_get: false },
    LogInputNode: { logger_name: '', level: 'INFO' },
    ProcessInputNode: { cmd: 'echo hello', interval: 2.0, timeout: 30.0, cwd: '' },
    PythonScriptInputNode: { script: 'print("hi")', interval: 2.0 },
    ShellInputNode: { command: 'date', interval: 2.0, timeout: 30.0, cwd: '' },
    JSONInputNode: { source: 'stdin' },

    FileOutputNode: { path: '' },
    LogOutputNode: { level: 'INFO', file: '' },
    JSONOutputNode: { pretty: true },
    DisplayNode: { prefix: '', suffix: '' },
    MQTTOutputNode: { broker: 'localhost', port: 1883, topic: 'pystreamflow/out', client_id: '', username: '', password: '', qos: 0, retain: false },
    ScriptedOutputNode: { script: 'return data' },
    WebOutputNode: { sse: true },
    WebOutputJSONNode: { sse: true },
    // See the identical note on ApiInputNode above - no host/port widgets.
    ApiOutputNode: { uri: '', sse: true },
    ProcessOutputNode: { cmd: 'cat', timeout: 30.0, cwd: '' },
    SocketOutputNode: { mode: 'client', host: '127.0.0.1', port: 9002, timeout: 10.0, append_newline: true },

    GrepNode: { pattern: '.*' },
    TemplateNode: { template: '{data}' },
    JSONModifyNode: { ops: [] },
    EncodingConvertNode: { input_encoding: 'utf-8', output_encoding: 'utf-8', decode_to_text: true },
    JSONExtractNode: { path: '' },
    ScriptNode: { script: 'pass' },

    AndNode: { inputs_needed: 2 },
    OrNode: { inputs_needed: 2 },
    NandNode: { inputs_needed: 2 },
    NorNode: { inputs_needed: 2 },
    XorNode: { inputs_needed: 2 },
    XnorNode: { inputs_needed: 2 },
    CompareNode: { operator: 'eq', compare_value: '', branch_on_result: false },
    MathNode: { op: 'add', value: 0 },

    NumericAddNode: { value: 0 },
    NumericSubNode: { value: 0 },
    NumericMulNode: { value: 1 },
    NumericDivNode: { value: 1 },
    NumericModNode: { value: 1 },
    NumericPowNode: { value: 2 },
    NumericMinNode: { value: 0 },
    NumericMaxNode: { value: 0 },
    NumericClampNode: { min: 0, max: 100 },
    NumericRoundNode: { ndigits: 0 },

    TextReplaceNode: { find: '', replace: '' },
    TextSubstringNode: { start: 0, end: null },
    TextStripNode: { chars: null },
    TextSplitNode: { sep: ' ', maxsplit: -1 },
    TextJoinNode: { sep: ' ' },

    LineSplitterNode: { keepends: false },
    TokenizerNode: { pattern: '\\S+' },
    LineBufferNode: { mode: 'multi', lines: 1, timeout: 0.5 },
    TrimStringNode: { chars: null },
    ListStringsNode: { strings: [], interval: 1.0, loop: true },
    TableNode: { columns: [], max_rows: 1000, emit_interval: 5.0 },

    TimerNode: { interval: 1.0, emit_payload: 'tick' },
    TriggerNode: { action: 'start', target_node_id: '', delay: 0 },
    TimerTriggerNode: { interval: 5.0, action: 'start', target_node_id: '' },
    TriggerOnNode: { target_node_id: '' },
    TriggerOffNode: { target_node_id: '' },
    TriggerPauseNode: { target_node_id: '' },
    TriggerIfNode: { condition: 'truthy', action: 'start', target_node_id: '', field: null },
    TriggerThresholdNode: { threshold: 10, action: 'start', target_node_id: '', reset: true },
    TriggerDebounceNode: { debounce: 1.0, action: 'start', target_node_id: '' },
    TriggerPulseNode: { interval: 5.0, pulse_count: 0, action: 'start', target_node_id: '' },
    TriggerToggleNode: { action_on: 'start', action_off: 'stop', target_node_id: '' },

    SubgraphNode: { workflow_path: '', input_node_id: '', output_node_id: '', input_port: 'in', output_port: 'out' },
    StackNode: { max_size: 1000, mode: 'push_pop' },
    FIFOQueueNode: { max_size: 1000, drop_policy: 'drop' },
    LIFOQueueNode: { max_size: 1000 },
    ClockNode: { interval: 1.0, start_value: 0, count_up: true, reset_on_input: false },
    HTMLScraperNode: { extractors: [], output_key: 'scraped' },
    Base64DecodeNode: { output_encoding: 'utf-8', strip_whitespace: true, validate_padding: true, output: 'auto' },
    Base64EncodeNode: { input_encoding: 'utf-8', output_encoding: 'utf-8', urlsafe: false, add_newlines: false, line_length: 76, data_url: false },
    UserPromptNode: { prompt: 'Enter value:', timeout: 300.0, default: '' },
    RollingWindowBufferNode: { max_size: 1000, retain_after_flush: true, emit_passthrough: true },

    // Feature request round: split-by-value routing, an MCP client, a
    // cron-scheduled source, a variable-input sync barrier, a queue+pop
    // gate, and a visual LED-activity relay - see each node's own Python
    // module docstring (pystreamflow/nodes/) for full behavior.
    //
    // `values` seeded with 4 placeholder entries (not []) so the 4
    // out0..out3 ports rebuildPorts() already creates for any DYNAMIC-
    // output node by default (DYNAMIC_SLOT_COUNT) start out matched to a
    // real, editable (if placeholder) value each, rather than looking
    // wired to nothing - the "+ value/output"/"− value/output" buttons
    // below then keep the two in lockstep from here on.
    SplitByValueNode: { values: ['value1', 'value2', 'value3', 'value4'], key: '', strict: false },
    // Bug fix found while adding HttpPostNode below (unrelated to that
    // feature): this default URL still pointed at '/mcp/messages', the
    // hand-rolled JSON-RPC endpoint that section 34's MCP SDK rewrite
    // removed entirely - a MCPClientNode dragged from the palette got
    // pre-filled with a dead URL. Corrected to match this node's own
    // real default (see nodes/mcp_client_node.py's init()) and the
    // actual SDK-served Streamable HTTP mount.
    MCPClientNode: { url: 'http://localhost:8000/mcp', tool: '', api_key: '', timeout: 30.0 },
    CronNode: { cron: '* * * * *' },
    SyncBarrierNode: { flush: true },
    QueueGateNode: { max_size: 0 },
    // RoundRobinNode: `count` seeded to 3 (rather than the generic
    // DYNAMIC_SLOT_COUNT of 4) since 3 is a clearer, more legible default
    // for a rotating-turns node to demonstrate on the canvas; rebuildPorts()'s
    // psfRoundRobin branch below seeds exactly `count` real out0..outN
    // ports from this value rather than the generic DYNAMIC default, and
    // the "+ output"/"− output" buttons keep the two in lockstep from
    // here on - same pattern as SplitByValueNode's `values` above.
    RoundRobinNode: { count: 3 },
    HttpPostNode: { url: 'https://example.com/webhook', method: 'POST', content_type: 'json', headers: {}, auth_token: '', timeout: 30.0 },
  };

  function definePsfNodeClass(key, meta) {
    function PSFNode() {
      // Deep-clone so mutating one node's properties (e.g. appending to
      // an array-valued field) never leaks into another node created
      // from the same DEFAULT_CONFIG entry.
      this.properties = JSON.parse(JSON.stringify(DEFAULT_CONFIG[meta.type] || {}));
      this.psfType = meta.type;
      this.psfKey = key;
      this.boxcolor = STATUS_COLORS.idle;
      this.resizable = true;
      rebuildPorts(this);
    }
    PSFNode.title = `${meta.icon} ${meta.label}`;
    PSFNode.desc = (nodeDocs[meta.type] || {}).desc || meta.label;
    PSFNode.psfIsTrigger = TRIGGER_RE.test(meta.type);

    PSFNode.prototype.onNodeCreated = function () {
      if (!this.psfId) this.psfId = genId();
      psfNodesById.set(this.psfId, this);
      addWidgetsForNode(this);
      syncAttributeSlots(this);
    };

    // Resolves the same edge "kind" priority the old UI's onmouseup used:
    // an attribute-target drop always wins; else a Trigger-family
    // source's output is always 'control'; only then do the two manual
    // override toggles (Trigger Wiring / Endpoint Wiring) get a say;
    // 'data' is the default. Runs once per newly-created (or removed)
    // link, on the *target* node (LiteGraph fires onConnectionsChange on
    // both ends, but only the target side has enough info - and needs to
    // run only once per link).
    //
    // Bug fix (feedback: "after starting a node via right click menu i
    // still need to hit the run button to let the generator node emit
    // outputs" / "only by clicking the run button some states are
    // updated (attribute changes including wiring changes)"): drawing (or
    // removing) a wire on the canvas used to be purely a frontend
    // LiteGraph-object change with no backend counterpart at all -
    // POST/DELETE /nodes/connect (api/server.py) was only ever called in
    // bulk, by runWorkflow() building a brand-new Session from the whole
    // current graph. So a node started ad-hoc via the right-click "Start"
    // action (or auto-started the moment it's dropped on the canvas - see
    // syncNodeToBackend()) ran with whatever wiring it had at that exact
    // moment - which, since wiring happens as a separate, later step on
    // the canvas, was normally none at all - and nothing that got wired
    // or rewired afterward reached it until "Run" threw the whole graph
    // away and rebuilt it as a Session. Mirroring syncNodeToBackend()'s
    // existing node-creation sync, every wire drawn or removed between two
    // *live* nodes now reaches the backend immediately via
    // syncWireToBackend() below - skipped during a bulk restore (Load
    // Demo/Session/Import, subgraph open/ungroup) via suppressWireSync,
    // exactly the same opt-out syncNodeToBackend() already has via
    // opts.syncBackend, since those still correctly wire everything at
    // once via the eventual Run action.
    PSFNode.prototype.onConnectionsChange = function (type, slot, connected, linkInfo) {
      if (type !== LiteGraph.INPUT || !linkInfo) return;
      const srcNode = graph ? graph.getNodeById(linkInfo.origin_id) : null;
      if (!connected) { syncWireToBackend('disconnect', srcNode, this, linkInfo, slot); return; }
      const inputSlot = this.inputs && this.inputs[slot];
      // Phase 4 of the wire-kind-unification design (see
      // claude/design_unified_wire_kinds_plan.md), Option A "drop-time
      // picker with a smart default": a one-shot "mark next wire as raw"
      // arm (set via right-click on an output slot - see
      // getSlotMenuOptions below) is consumed here the moment ANY wire is
      // drawn from the exact (node, output-slot) it was armed on, whether
      // or not it actually ends up applying below - an attribute/control
      // target, or a Trigger-family source, still wins outright, same
      // priority order as before this phase. Consuming it unconditionally
      // here (not only in the branch that actually uses it) is what makes
      // it a true one-shot instead of a mode that could linger and
      // surprise-tag some later, unrelated wire from the same anchor.
      let rawArmedHere = false;
      if (srcNode && state.rawArmed && state.rawArmed.nodeId === srcNode.psfId && state.rawArmed.slot === linkInfo.origin_slot) {
        rawArmedHere = true;
        state.rawArmed = null;
      }
      let kind = 'data';
      if (inputSlot && inputSlot.psfKind === 'attribute') kind = 'attribute';
      // A wire dropped on the dedicated control slot (see rebuildPorts())
      // is unambiguously a control edge regardless of its source - this
      // takes priority over the Trigger-family-source/raw-arm/manual-toggle
      // heuristics below, which exist for the (now legacy, but still
      // supported for old saved workflows) case of a control wire landing
      // on an ordinary data slot instead.
      else if (inputSlot && inputSlot.psfKind === 'control') kind = 'control';
      else if (srcNode && srcNode.constructor && srcNode.constructor.psfIsTrigger) kind = 'control';
      else if (rawArmedHere) kind = 'raw';
      else if (state.triggerMode) kind = 'control';
      else if (state.endpointMode) kind = 'endpoint';
      if (rawArmedHere && kind !== 'raw') {
        // The armed anchor's wire landed somewhere that overrides raw
        // (an attribute/control target, or this was actually a
        // Trigger-family node's output) - tell the user why their arming
        // request didn't visibly take effect, rather than leaving them to
        // wonder why the wire isn't teal.
        toast(`That wire connected as '${kind}' instead of raw (target/source overrides it)`);
      }
      linkInfo.psfKind = kind;
      linkInfo.color = KIND_COLORS[kind];
      syncWireToBackend('connect', srcNode, this, linkInfo, slot);
    };

    // Phase 4's secondary raw-wiring affordance: right-click a real
    // output slot for "Mark next wire from here as raw" (or "Cancel..."
    // if already armed) instead of a pre-armed toolbar mode - see the
    // `state.rawArmed` comment above and onConnectionsChange above for
    // how the arm is applied and consumed. Overriding this LiteGraph hook
    // replaces its *entire* default slot-context-menu (see the vendored
    // library's processContextMenu), so the default "Disconnect Links" /
    // "Remove Slot" / "Rename Slot" entries are rebuilt verbatim here too
    // - matched by the exact same `content` strings the library's own
    // shared menu callback dispatches on, so those three keep behaving
    // exactly as before for every node, not just ones without this hook.
    PSFNode.prototype.getSlotMenuOptions = function (slotInfo) {
      const opts = [];
      if (slotInfo.output && slotInfo.output.links && slotInfo.output.links.length) {
        opts.push({ content: 'Disconnect Links', slot: slotInfo });
      }
      const slotDef = slotInfo.input || slotInfo.output;
      if (slotDef.removable) opts.push(slotDef.locked ? 'Cannot remove' : { content: 'Remove Slot', slot: slotInfo });
      if (!slotDef.nameLocked) opts.push({ content: 'Rename Slot', slot: slotInfo });
      // Only offered on a real output slot, and never on a Trigger-family
      // node's output - onConnectionsChange above always resolves that to
      // 'control' regardless of any raw arm, so offering to arm it would
      // be a control that silently never does anything.
      if (slotInfo.output && !(this.constructor && this.constructor.psfIsTrigger)) {
        const armedHere = state.rawArmed && state.rawArmed.nodeId === this.psfId && state.rawArmed.slot === slotInfo.slot;
        if (armedHere) {
          opts.push({
            content: '❌ Cancel raw-wire arming',
            callback: () => { state.rawArmed = null; toast('Raw-wire arming cancelled'); },
          });
        } else {
          opts.push({
            content: '🔗 Mark next wire from here as raw',
            callback: () => {
              state.rawArmed = { nodeId: this.psfId, slot: slotInfo.slot };
              toast(`Next wire from '${slotInfo.output.name}' will be raw`);
            },
          });
        }
      }
      return opts;
    };

    PSFNode.prototype.getExtraMenuOptions = function (canvas, options) {
      if (!this.psfId) return;
      options.push(
        null,
        { content: '▶ Start', callback: () => nodeAction(this.psfId, 'start') },
        { content: '⏹ Stop', callback: () => nodeAction(this.psfId, 'stop') },
        { content: '⏸ Pause / Resume', callback: () => nodeAction(this.psfId, 'pause') },
        { content: '⏭ Step', callback: () => nodeAction(this.psfId, 'step') },
        // POST /nodes/{id}/reset -> BaseNode.reset(): clears this node's
        // own accumulated state (stats/error bookkeeping, plus a stack/
        // queue/table's actual contents for the node types that override
        // it) without stopping or restarting it. Uses the exact same
        // generic nodeAction() as Start/Stop/Pause/Step above - no
        // special-casing needed since /nodes/{id}/reset follows the same
        // fire-and-forget control-message shape as /step.
        { content: '♻ Reset', callback: () => nodeAction(this.psfId, 'reset') },
        // Same "output nodes don't need emit buttons" gate as the
        // title-bar icon (onDrawTitleText/onMouseDown above) - keeping
        // this menu entry for an Outputs-group node while the title-bar
        // twin is hidden there would just be a second, more-hidden way to
        // do the same pointless thing on the same node.
        ...(keyGroup[this.psfKey] === 'Outputs' ? [] : [{ content: '➤ Manual emit', callback: () => manualEmitNode(this) }]),
        { content: '👁 Live view…', callback: () => openLiveModal(this) },
        { content: '📋 Duplicate', callback: () => duplicatePsfNode(this) },
        { content: '💾 Save as template', callback: () => saveTemplate(this) },
        { content: '🧬 Advanced JSON…', callback: () => openAdvancedModal(this) },
      );
      if (this.psfType === 'SubgraphNode') {
        options.push(
          { content: '📂 Open Subgraph', callback: () => openSubgraphNode(this) },
          { content: '📤 Ungroup Subgraph', callback: () => ungroupSubgraphNode(this) },
        );
      }
      const doc = nodeDocs[this.psfType];
      if (doc && doc.url) options.push({ content: '📖 Docs', callback: () => window.open(doc.url, '_blank') });
    };

    PSFNode.prototype.onDrawForeground = function (ctx) {
      if (this.flags && this.flags.collapsed) return;
      // Phase 4's raw-arm indicator: a persistent (not fading, unlike the
      // data-flow flash below) dashed ring in the raw wire color for
      // whichever node currently has an output slot armed via
      // getSlotMenuOptions' "Mark next wire from here as raw" - without
      // this, the arm would be an invisible state a person would have to
      // trust blindly between the toast firing and actually drawing the
      // wire. Cleared the moment that wire is drawn (or the arm is
      // cancelled - see onConnectionsChange/getSlotMenuOptions above), so
      // this never lingers past the one wire it was meant for.
      if (this.psfId && state.rawArmed && state.rawArmed.nodeId === this.psfId) {
        ctx.save();
        ctx.strokeStyle = KIND_COLORS.raw;
        ctx.lineWidth = 2;
        ctx.setLineDash([4, 3]);
        const rawPad = 5;
        ctx.strokeRect(-rawPad, -rawPad, this.size[0] + rawPad * 2, this.size[1] + rawPad * 2);
        ctx.restore();
      }
      // Glow-border flash: "add a glowing border effect (just a flash)
      // to nodes which received input or emitted output further down the
      // chain" - see nodeFlashUntil/flashNode()/propagateFlash() above
      // pollNodeStatus(). Drawn as a soft blurred stroke just outside the
      // node body, fading out over FLASH_MS from full brightness at the
      // moment it was triggered - independent of (and drawn regardless
      // of) the progress-bar code below, so a node with both an active
      // progress bar and a fresh flash shows both at once.
      const flashUntil = this.psfId ? (nodeFlashUntil[this.psfId] || 0) : 0;
      const remaining = flashUntil - Date.now();
      if (remaining > 0) {
        const alpha = Math.min(1, remaining / FLASH_MS);
        ctx.save();
        ctx.shadowColor = `rgba(56, 189, 248, ${0.9 * alpha})`;
        ctx.shadowBlur = 16 * alpha;
        ctx.strokeStyle = `rgba(125, 211, 252, ${0.9 * alpha})`;
        ctx.lineWidth = 3;
        const pad = 3;
        ctx.strokeRect(-pad, -pad, this.size[0] + pad * 2, this.size[1] + pad * 2);
        ctx.restore();
      }
      // LedActivityNode: "add a 'led activity' node ... when it relays it
      // blinks its 'led' (more of a visual gadget)" - a real physical
      // patch-panel-style indicator, distinct from (and drawn in addition
      // to) the generic whole-node glow-border flash every node gets
      // above. Reuses that exact same `remaining`/flash timer already
      // computed above (this node's own emits already trip the generic
      // "did a genuinely new item arrive" detection in pollNodeStatus()
      // that drives it) rather than tracking a second, separate timer.
      if (this.psfType === 'LedActivityNode') {
        const lit = remaining > 0;
        const r = 5;
        const cx = this.size[0] - 16;
        const cy = -0.5 * (LiteGraph.NODE_TITLE_HEIGHT || 24);
        ctx.save();
        ctx.beginPath();
        ctx.arc(cx, cy, r, 0, Math.PI * 2);
        ctx.fillStyle = lit ? '#ef4444' : '#450a0a';
        ctx.shadowColor = lit ? '#f87171' : 'transparent';
        ctx.shadowBlur = lit ? 10 : 0;
        ctx.fill();
        ctx.shadowBlur = 0;
        ctx.strokeStyle = '#7f1d1d';
        ctx.lineWidth = 1;
        ctx.stroke();
        ctx.restore();
      }
      const prog = nodeProgressMap.get(this.psfId);
      if (prog == null) return;
      const w = this.size[0];
      const h = this.size[1];
      const barY = h - 8;
      ctx.fillStyle = '#0f172a';
      ctx.fillRect(4, barY, w - 8, 4);
      ctx.fillStyle = '#3b82f6';
      ctx.fillRect(4, barY, Math.max(0, (w - 8) * prog / 100), 4);
    };

    // Manual-emit title-bar button: a small always-visible icon in the
    // title bar's top-right corner, on every node (not just the ones
    // that happen to be selected/right-clicked) - the request was
    // specifically for something visible "in their title on the right"
    // rather than one more entry buried in the right-click menu (which
    // already had Step/Start/Stop etc. and is where this lived before).
    // onDrawTitleText is additive - LiteGraph still draws its own default
    // title background/text around this call (see the vendored library's
    // drawNode()) - so this only adds the icon, it doesn't reimplement
    // title-bar rendering. The matching hit-test lives in onMouseDown
    // below, in the exact top-right NODE_TITLE_HEIGHT-square box
    // LiteGraph's own built-in "open subgraph" button uses for real
    // LGraph subgraph nodes (see the vendored library's mouse-down
    // handling) - safe to reuse here since none of this app's PSFNode
    // instances set the native `.subgraph` property that button is
    // gated on (SubgraphNode implements its own separate open/close UI).
    PSFNode.prototype.onDrawTitleText = function (ctx, titleHeight) {
      // Feedback: "output nodes don't need emit buttons" - manual emit
      // replays a node's last item(s) onto whatever is wired downstream
      // of it, which is meaningful for a source/modifier/logic node but
      // pointless clutter on the palette's own "Outputs" group (Display,
      // Log/File/JSON/Web/API/Process/Socket/MQTT/Scripted Output): these
      // are the terminal, sink end of a workflow in the mental model the
      // node palette itself teaches (nodeGroups.Outputs below), even
      // though a couple of them (WebOutput, ApiOutput, ...) technically
      // still have a real 'out' port for advanced chaining. Purely a
      // rendering-layer gate, exactly like SHOW_RAW_PORTS above - nothing
      // about manual_emit()/POST /nodes/{id}/emit itself changes, so this
      // is trivially reversible and every node keeps replaying correctly
      // if triggered another way (e.g. directly via the API/MCP tool).
      if (!this.psfId || (this.flags && this.flags.collapsed) || keyGroup[this.psfKey] === 'Outputs') return;
      const cx = this.size[0] - titleHeight / 2;
      const cy = -titleHeight / 2;
      ctx.save();
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.font = `${Math.round(titleHeight * 0.6)}px sans-serif`;
      ctx.fillStyle = '#e2e8f0';
      ctx.fillText('➤', cx, cy + 1);
      ctx.restore();
    };
    PSFNode.prototype.onMouseDown = function (e, localpos) {
      // Matching gate for onDrawTitleText above - the button isn't drawn
      // for an Outputs-group node, so its hit-test must also stand down
      // there, or the (invisible) icon would still swallow that
      // top-right-corner click and eat the drag.
      if (!this.psfId || (this.flags && this.flags.collapsed) || keyGroup[this.psfKey] === 'Outputs') return false;
      const th = LiteGraph.NODE_TITLE_HEIGHT;
      const hit = localpos[1] < 0 && localpos[1] > -th
        && localpos[0] > this.size[0] - th && localpos[0] < this.size[0];
      if (!hit) return false;
      manualEmitNode(this);
      return true; // handled - don't also start a node drag from this click
    };

    LiteGraph.registerNodeType(fullTypeForKey(key), PSFNode);
    return PSFNode;
  }

  function buildNodeClasses() {
    Object.entries(nodeTypes).forEach(([key, meta]) => definePsfNodeClass(key, meta));
  }

  // ------------------------------------------------------------------
  // Subgraph boundary pseudo-nodes ("edge points")
  // ------------------------------------------------------------------
  // Bug fix (feedback: "subgraph edge points (inside subgraph editor)
  // cannot be wired to node inputs / outputs inside the subgraph"):
  // opening a subgraph (openSubgraphNode() below) used to load only the
  // real inner nodes - nodes/subgraph.py's input_bridges/output_bridges
  // (which map one of the SubgraphNode's own external ports, as seen from
  // the *parent* graph, onto one inner node's port) had no visual
  // representation at all inside the subgraph editor, so there was
  // nothing on that canvas to drag a wire onto or from to see or change
  // which inner port a given external port actually reaches - the only
  // way to set that mapping was hand-editing the input_bridges/
  // output_bridges JSON.
  //
  // These two node types are the fix: purely client-side editing aids
  // (LiteGraph.registerNodeType() directly, not definePsfNodeClass() -
  // they never get a psfId and are never POSTed to the backend as real
  // nodes) that addSubgraphBoundaryNodes() drops into the inner graph
  // for the duration of editing, one output port per current
  // input_bridges entry (this subgraph's inputs, arriving from outside)
  // and one input port per current output_bridges entry (its outputs,
  // leaving to outside) - each pre-wired to the inner node/port the
  // bridge already names, so the existing mapping is immediately visible
  // as a real wire, exactly like any other connection on the canvas.
  // Dragging that wire elsewhere, or using "+ port"/"- port" to add or
  // remove an external port entirely, edits the mapping right there;
  // collectBoundaryBridges() (used by closeSubgraphFrame() below) reads
  // the result back off these two nodes' current wiring when the
  // subgraph is saved, and removeSubgraphBoundaryNodes() deletes them
  // again first so they never leak into the saved workflow file itself
  // (graphToWorkflow() would otherwise serialize them as ordinary nodes
  // with an unregistered type, which nodes/subgraph.py's SubgraphNode.
  // init() silently turns into an inert no-op GenericNode stub forever).
  const SUBGRAPH_BOUNDARY_IN_TYPE = 'psf/subgraph_boundary_in';
  const SUBGRAPH_BOUNDARY_OUT_TYPE = 'psf/subgraph_boundary_out';

  function SubgraphBoundaryInNode() {
    this.title = '⬅ Subgraph Inputs';
    this.color = '#334155';
    this.bgcolor = '#0f172a';
    this.addWidget('button', '+ port', null, () => {
      const name = window.prompt('New external input port name:', 'in' + this.outputs.length);
      if (!name) return;
      if ((this.outputs || []).some((o) => o.name === name)) { toast(`Already have an input port named "${name}"`); return; }
      this.addOutput(name, 0);
      if (this.__psfOuterNode) this.__psfOuterNode.addInput(name, 0);
      if (graphcanvas) graphcanvas.setDirty(true, true);
    });
    this.addWidget('button', '− port', null, () => {
      if (!this.outputs || !this.outputs.length) return;
      const removed = this.outputs[this.outputs.length - 1];
      this.removeOutput(this.outputs.length - 1);
      if (this.__psfOuterNode) {
        const idx = findInputSlotIndex(this.__psfOuterNode, removed.name);
        if (idx >= 0) this.__psfOuterNode.removeInput(idx);
      }
      if (graphcanvas) graphcanvas.setDirty(true, true);
    });
  }
  SubgraphBoundaryInNode.title = 'Subgraph Inputs';
  LiteGraph.registerNodeType(SUBGRAPH_BOUNDARY_IN_TYPE, SubgraphBoundaryInNode);

  function SubgraphBoundaryOutNode() {
    this.title = '➡ Subgraph Outputs';
    this.color = '#334155';
    this.bgcolor = '#0f172a';
    this.addWidget('button', '+ port', null, () => {
      const name = window.prompt('New external output port name:', 'out' + this.inputs.length);
      if (!name) return;
      if ((this.inputs || []).some((i) => i.name === name)) { toast(`Already have an output port named "${name}"`); return; }
      this.addInput(name, 0);
      if (this.__psfOuterNode) this.__psfOuterNode.addOutput(name, 0);
      if (graphcanvas) graphcanvas.setDirty(true, true);
    });
    this.addWidget('button', '− port', null, () => {
      if (!this.inputs || !this.inputs.length) return;
      const removed = this.inputs[this.inputs.length - 1];
      this.removeInput(this.inputs.length - 1);
      if (this.__psfOuterNode) {
        const idx = findOutputSlotIndex(this.__psfOuterNode, removed.name);
        if (idx >= 0) this.__psfOuterNode.removeOutput(idx);
      }
      if (graphcanvas) graphcanvas.setDirty(true, true);
    });
  }
  SubgraphBoundaryOutNode.title = 'Subgraph Outputs';
  LiteGraph.registerNodeType(SUBGRAPH_BOUNDARY_OUT_TYPE, SubgraphBoundaryOutNode);

  // Drops both boundary nodes into `innerGraph` (the freshly-loaded
  // subgraph, before graphcanvas.openSubgraph(sub) switches the visible
  // canvas to it), one port per bridge already configured on
  // `outerNode` (the SubgraphNode instance living in the *parent*
  // graph), wired to whichever inner node/port each bridge currently
  // names - so the existing mapping renders as a real, visible,
  // draggable wire the moment the subgraph opens.
  function addSubgraphBoundaryNodes(outerNode, innerGraph) {
    const props = outerNode.properties || {};
    const inputBridges = Array.isArray(props.input_bridges) ? props.input_bridges : [];
    const outputBridges = Array.isArray(props.output_bridges) ? props.output_bridges : [];

    const boundsIn = LiteGraph.createNode(SUBGRAPH_BOUNDARY_IN_TYPE);
    boundsIn.__psfOuterNode = outerNode;
    boundsIn.pos = [40, 80];
    innerGraph.add(boundsIn);
    inputBridges.forEach((b) => boundsIn.addOutput(b.external_port, 0));

    const boundsOut = LiteGraph.createNode(SUBGRAPH_BOUNDARY_OUT_TYPE);
    boundsOut.__psfOuterNode = outerNode;
    boundsOut.pos = [900, 80];
    innerGraph.add(boundsOut);
    outputBridges.forEach((b) => boundsOut.addInput(b.external_port, 0));

    // Wire each bridge to the inner node/port it currently names -
    // suppressWireSync doesn't need to guard these .connect() calls the
    // way loadWorkflowIntoGraph()'s do: neither boundary node is a
    // PSFNode, so LiteGraph never calls a syncWireToBackend()-driving
    // onConnectionsChange for either end of these links regardless, and
    // the real inner node's own onConnectionsChange (fired when it's the
    // connection's target) already no-ops safely on a srcNode with no
    // psfId (see syncWireToBackend()'s own guard).
    inputBridges.forEach((b, i) => {
      const innerNode = psfNodesById.get(b.node_id);
      if (!innerNode) return;
      const slot = findInputSlotIndex(innerNode, b.port);
      if (slot >= 0) boundsIn.connect(i, innerNode, slot);
    });
    outputBridges.forEach((b, i) => {
      const innerNode = psfNodesById.get(b.node_id);
      if (!innerNode) return;
      const slot = findOutputSlotIndex(innerNode, b.port);
      if (slot >= 0) innerNode.connect(slot, boundsOut, i);
    });
  }

  // Reads the current input_bridges/output_bridges back off the two
  // boundary nodes' live wiring - called right before they're stripped
  // out of the graph on save, so this is the definitive source for what
  // the user actually wired up while the subgraph was open, whether or
  // not it matches what the bridges looked like when it was opened.
  function collectBoundaryBridges(innerGraph) {
    const boundsIn = innerGraph._nodes.find((n) => n.type === SUBGRAPH_BOUNDARY_IN_TYPE);
    const boundsOut = innerGraph._nodes.find((n) => n.type === SUBGRAPH_BOUNDARY_OUT_TYPE);
    const inputBridges = [];
    (boundsIn && boundsIn.outputs ? boundsIn.outputs : []).forEach((port) => {
      const linkId = (port.links || [])[0];
      const link = linkId != null ? innerGraph.links[linkId] : null;
      if (!link) return;
      const tgt = innerGraph.getNodeById(link.target_id);
      if (!tgt || !tgt.psfId) return;
      const tp = tgt.inputs && tgt.inputs[link.target_slot];
      inputBridges.push({ external_port: port.name, node_id: tgt.psfId, port: (tp && tp.name) || 'in' });
    });
    const outputBridges = [];
    (boundsOut && boundsOut.inputs ? boundsOut.inputs : []).forEach((port) => {
      const linkId = port.link;
      const link = linkId != null ? innerGraph.links[linkId] : null;
      if (!link) return;
      const src = innerGraph.getNodeById(link.origin_id);
      if (!src || !src.psfId) return;
      const sp = src.outputs && src.outputs[link.origin_slot];
      outputBridges.push({ external_port: port.name, node_id: src.psfId, port: (sp && sp.name) || 'out' });
    });
    return { inputBridges, outputBridges };
  }

  function removeSubgraphBoundaryNodes(innerGraph) {
    innerGraph._nodes.slice().forEach((n) => {
      if (n.type === SUBGRAPH_BOUNDARY_IN_TYPE || n.type === SUBGRAPH_BOUNDARY_OUT_TYPE) innerGraph.remove(n);
    });
  }

  // ------------------------------------------------------------------
  // Node creation / duplication / templates
  // ------------------------------------------------------------------

  // Bug fix: "starting a node while running is not working". Root cause,
  // found by tracing what POST /nodes/{id}/start actually needs (a real
  // backend node registered in core/web_server.py's `_nodes`) against
  // what happens when a node is dropped on the canvas: createPsfNode()
  // (called by every palette-button click, and by LiteGraph's own
  // built-in double-click/right-click "Add Node" search) only ever
  // built the *frontend* LiteGraph node - nothing here ever called
  // POST /nodes to create the matching backend one. duplicatePsfNode()
  // was the one exception (it made its own explicit POST after calling
  // createPsfNode()) - which is exactly why the backend's own
  // create_node() handler already auto-starts a freshly-created node and
  // talks about "a node added to a live graph this way (dragged from the
  // palette / duplicated onto a running session...)" as if the palette
  // path already did this; it never actually did. So: build a workflow,
  // press Run (which *does* create real backend nodes, via
  // POST /workflows -> /sessions -> /sessions/{id}/start), then drag one
  // more node onto that now-running canvas and right-click -> Start on
  // it - `_nodes.get(node_id)` finds nothing, the request silently comes
  // back `{"error": "node not found"}`, and nodeAction() doesn't even
  // surface that error to the UI (see its own fetch(...).catch(() => {})),
  // so absolutely nothing visibly happens. Any of Start/Stop/Pause/Step/
  // Manual-emit on such a node fails exactly the same way, for the exact
  // same reason.
  //
  // Fixed by finally making createPsfNode() do what create_node()'s own
  // comment already assumed: sync every node it creates to the backend
  // via POST /nodes, `opts.syncBackend` (default true) letting the two
  // *bulk* load paths - loadWorkflowIntoGraph() (Load Demo/Session/
  // Import) and ungroupSubgraphNode() - opt out and keep their existing,
  // deliberate behavior of staging nodes on the canvas only, with real
  // backend nodes+wiring created all at once by the eventual Run action
  // (each node individually POSTed ahead of that, unwired, would just be
  // duplicate work immediately superseded - and, worse, would collide
  // node ids with the Session's own instantiation of the same workflow).
  function syncNodeToBackend(node) {
    fetch('/nodes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ node_id: node.psfId, node_type: node.psfType, config: node.properties || {} }),
    }).catch(() => {});
  }

  // Set true for the duration of a bulk graph restore (loadWorkflowIntoGraph()/
  // ungroupSubgraphNode()) so the many s.connect() calls those make don't
  // each fire a live /nodes/connect request too - those nodes are created
  // with syncBackend:false and have no backend counterpart yet at that
  // point anyway (real backend nodes+wiring get created all at once by the
  // eventual Run action, same reasoning as syncNodeToBackend()'s own
  // opts.syncBackend), so every one of those calls would otherwise just be
  // a wasted request guaranteed to fail with "source or target not found".
  let suppressWireSync = false;

  // Bug found in a codebase-wide audit for unimplemented/inert
  // functionality: removing a node from the canvas - Delete/Backspace,
  // the right-click "Remove" menu item, or any other path that ends up
  // calling LiteGraph's own graph.remove(node) - never told the backend
  // at all. The real node (its open HTTP routes if any, subprocess,
  // MQTT/MCP connection, attribute-pump tasks, ...) kept running forever,
  // orphaned and unreachable from the canvas - a resource leak in the
  // exact same "the canvas and the backend silently drift apart" shape
  // section 18 already fixed for wiring (see syncWireToBackend above)
  // and node creation (see createPsfNode's own syncBackend flag), just
  // never closed for deletion. graph.onNodeRemoved (set on the main
  // outer graph object only, in init() below) is the single hook every
  // one of those removal paths already funnels through - including the
  // subgraph group/ungroup helpers' own graph.remove() calls, which have
  // the identical leak today (grouping a selection into a subgraph, or
  // ungrouping one, orphans the replaced nodes' real backend instances
  // exactly the same way).
  let suppressNodeDeleteSync = false;

  // suppressNodeDeleteSync guards the one case where graph.remove() does
  // NOT mean "this node's backend instance should die": loadWorkflowIntoGraph()'s
  // graph.clear() at the top of a bulk Load Demo/Session/Import, which is
  // swapping what the *canvas* displays, not asking to tear down whatever
  // session or nodes were previously shown - firing a DELETE per
  // previously-displayed node there would race with (and could destroy)
  // a session that's still genuinely running.
  function syncNodeDeleteToBackend(node) {
    if (suppressNodeDeleteSync || !node || !node.psfId) return;
    fetch(`/nodes/${node.psfId}`, { method: 'DELETE' }).catch(() => {});
  }

  // See PSFNode.prototype.onConnectionsChange's comment above for why this
  // exists: keeps a live node's actual backend wiring in sync with what's
  // drawn on the canvas, immediately, instead of only at "Run".
  function syncWireToBackend(action, srcNode, tgtNode, linkInfo, targetSlot) {
    if (suppressWireSync || !srcNode || !tgtNode || !srcNode.psfId || !tgtNode.psfId) return;
    const sourceSlot = srcNode.outputs && srcNode.outputs[linkInfo.origin_slot];
    const inputSlot = tgtNode.inputs && tgtNode.inputs[targetSlot];
    const body = {
      source_id: srcNode.psfId,
      source_port: (sourceSlot && sourceSlot.name) || 'out',
      target_id: tgtNode.psfId,
      target_port: (inputSlot && inputSlot.name) || 'in',
      type: linkInfo.psfKind || 'data',
    };
    // Bug found while building Phase 4's new 'raw' wire-kind picker (see
    // claude/design_unified_wire_kinds_plan.md): this used to be a bare
    // fire-and-forget fetch with only a network-level .catch() - a real,
    // *rejected* connect (validate_edge() returning an error, e.g. this
    // build's own `type: 'raw'` against a DYNAMIC-schema source like
    // ForkNode) comes back as an ordinary 200 OK `{"error": "..."}` body,
    // which was never even read. The wire stayed drawn on the canvas
    // looking perfectly normal while the backend never actually wired
    // anything - a silently inert wire, exactly the "canvas and backend
    // drift apart" failure mode this whole engagement has spent months
    // eliminating elsewhere (see syncNodeToBackend/syncNodeDeleteToBackend's
    // own comments above for two earlier instances of the same bug
    // class). Now a rejected connect is surfaced via toast and the
    // just-drawn link is torn back off the canvas, so what's drawn always
    // matches what's actually wired. A rejected *disconnect* has nothing
    // to revert (the link is already gone from the canvas either way -
    // that's what triggered the DELETE), so this only reacts to a failed
    // 'connect'.
    fetchJSON('/nodes/connect', {
      method: action === 'connect' ? 'POST' : 'DELETE',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    }).then((data) => {
      if (action === 'connect' && data && data.error) {
        toast(`Connect rejected: ${data.error}`);
        if (tgtNode.disconnectInput) tgtNode.disconnectInput(targetSlot);
      }
    }).catch(() => {});
  }

  function createPsfNode(psfTypeOrKey, opts) {
    opts = opts || {};
    const key = nodeTypes[psfTypeOrKey] ? psfTypeOrKey : keyForType(psfTypeOrKey);
    if (!key) return null;
    const meta = nodeTypes[key];
    // Media plan phase 3: the search box and pasted/duplicated nodes come
    // through here too, not just the (greyed-out) palette button.
    if (unavailableTypes[meta.type] && opts.syncBackend !== false) {
      toast(`${meta.label}: ${unavailableTypes[meta.type]}`);
      return null;
    }
    // Bug fix (found while investigating why nodes rendered with no
    // config widgets at all - the concrete complaint behind "the MQTT
    // nodes need input fields to directly set host/user/pw/topic"):
    // this used to set the new node's `properties` to *only* whatever
    // opts.config/nodeTemplates supplied - `{}` for the overwhelmingly
    // common case of a fresh node dragged from the palette (no saved
    // template, no explicit config). LiteGraph.createNode's third
    // argument does `for (var f in c) e[f] = c[f]` *after* the PSFNode
    // constructor above has already set `this.properties` to a correct
    // deep clone of DEFAULT_CONFIG[meta.type] - so passing `properties: {}`
    // silently wiped out every one of that type's default fields right
    // before onNodeCreated() -> addWidgetsForNode() ran and built widgets
    // from (now-empty) node.properties. The result, verified live: every
    // node type's rich DEFAULT_CONFIG entry (broker/port/topic/... for
    // MQTTInputNode, and the equivalent for every other type) was already
    // correct and already read by the node's own init() - it just never
    // rendered as an editable widget for a node created this way, which is
    // how the overwhelming majority of nodes on any canvas are created.
    // Merging onto the real defaults instead of replacing them fixes
    // every node type at once, and still lets an explicit opts.config or
    // saved template override individual keys as before.
    const base = JSON.parse(JSON.stringify(DEFAULT_CONFIG[meta.type] || {}));
    const overrides = opts.config ? JSON.parse(JSON.stringify(opts.config))
      : (nodeTemplates[meta.type] ? JSON.parse(JSON.stringify(nodeTemplates[meta.type])) : {});
    const config = Object.assign(base, overrides);
    const node = LiteGraph.createNode(fullTypeForKey(key), null, {
      psfId: opts.id || genId(),
      properties: config,
    });
    if (!node) return null;
    if (opts.label) node.title = opts.label;
    node.pos = [opts.x != null ? opts.x : 200 + Math.random() * 200, opts.y != null ? opts.y : 200 + Math.random() * 200];
    graph.add(node);
    if (opts.syncBackend !== false) syncNodeToBackend(node);
    return node;
  }

  function duplicatePsfNode(node) {
    // No separate POST /nodes here any more - createPsfNode() now does
    // this for every node it creates by default (see its own comment),
    // so a second explicit call here would just be redundant.
    createPsfNode(node.psfKey, {
      config: node.properties,
      x: node.pos[0] + 30,
      y: node.pos[1] + 30,
      label: node.title,
    });
  }

  function saveTemplate(node) {
    nodeTemplates[node.psfType] = JSON.parse(JSON.stringify(node.properties || {}));
    localStorage.setItem('psf_node_templates', JSON.stringify(nodeTemplates));
    toast(`Template saved for ${node.psfType}`);
  }

  // ------------------------------------------------------------------
  // Graph <-> backend workflow JSON
  // ------------------------------------------------------------------
  function graphToWorkflow() {
    const nodesOut = graph._nodes.map((n) => ({
      id: n.psfId, type: n.psfType, label: n.title || n.psfType, config: n.properties || {}, x: n.pos[0], y: n.pos[1],
    }));
    const edgesOut = [];
    Object.values(graph.links || {}).forEach((link) => {
      if (!link) return;
      const s = graph.getNodeById(link.origin_id);
      const t = graph.getNodeById(link.target_id);
      if (!s || !t) return;
      const sp = (s.outputs && s.outputs[link.origin_slot] && s.outputs[link.origin_slot].name) || 'out';
      const tp = (t.inputs && t.inputs[link.target_slot] && t.inputs[link.target_slot].name) || 'in';
      edgesOut.push({ source: s.psfId, target: t.psfId, source_port: sp, target_port: tp, type: link.psfKind || 'data' });
    });
    return { nodes: nodesOut, edges: edgesOut };
  }

  // POST /workflows (used by runWorkflow() below) constructs a real
  // core/models.py Node(**n)/Edge(**e) dataclass per entry - Node only
  // has id/type/config (no label/x/y) and Edge only has
  // source/target/source_port/target_port/type, so anything extra in the
  // payload raises a hard 500 (verified: sending the same shape
  // graphToWorkflow() produces for YAML export - which includes label/x/y
  // for a nicer round-trip through Import/Export/Load Session, all of
  // which go through /parse_yaml instead and never construct a Node
  // object - throws "Node.__init__() got an unexpected keyword argument
  // 'label'"). The old hand-rolled ui.html knew this and built a
  // separate, deliberately minimal object for this one call; this does
  // the same rather than changing graphToWorkflow()'s richer shape.
  function graphToRunPayload() {
    const wf = graphToWorkflow();
    return {
      nodes: wf.nodes.map((n) => ({ id: n.id, type: n.type, config: n.config })),
      edges: wf.edges.map((e) => ({ source: e.source, target: e.target, source_port: e.source_port, target_port: e.target_port, type: e.type })),
    };
  }

  function loadWorkflowIntoGraph(nodesData, edgesData) {
    // graph.clear() below calls LiteGraph's own node.remove() on every
    // node currently on the canvas, which would otherwise fire a real
    // DELETE /nodes/{id} for each one via the onNodeRemoved hook set in
    // init() - this is swapping what the *canvas displays* (Load
    // Demo/Session/Import), not a request to tear down whatever session
    // was previously shown, so that flood of spurious deletes needs to
    // be suppressed specifically around this call.
    suppressNodeDeleteSync = true;
    try {
      graph.clear();
    } finally {
      suppressNodeDeleteSync = false;
    }
    psfNodesById.clear();
    const idMap = new Map();
    // Workflows saved/exported by this editor always carry x/y (see
    // graphToWorkflow()), but ones written by hand or by the CLI/MCP
    // side usually don't - fall back to a simple left-to-right, wrapping
    // grid instead of everyone's default random-ish spot (createPsfNode's
    // own fallback), which for a hand-written multi-node YAML tends to
    // drop every positionless node in virtually the same spot, stacked
    // on top of each other.
    let autoIdx = 0;
    (nodesData || []).forEach((n) => {
      if (!keyForType(n.type)) return; // unknown node type - nothing registered to create
      const hasPos = n.x != null && n.y != null;
      const x = hasPos ? n.x : 120 + (autoIdx % 4) * 260;
      const y = hasPos ? n.y : 100 + Math.floor(autoIdx / 4) * 170;
      if (!hasPos) autoIdx += 1;
      // syncBackend: false - this is a bulk stage-onto-canvas load (Load
      // Demo/Session/Import), not a live single-node add; real backend
      // nodes + wiring get created all at once, correctly wired, by the
      // eventual Run action (see syncNodeToBackend()'s comment above
      // createPsfNode() for why POSTing each one individually here would
      // be redundant and would collide ids with the Session's own copy).
      const node = createPsfNode(n.type, { id: n.id, config: n.config || {}, x, y, label: n.label, syncBackend: false });
      if (node) idMap.set(n.id, node);
    });
    // suppressWireSync: this is a bulk stage-onto-canvas restore, same
    // reasoning as syncBackend:false just above - these nodes have no
    // backend counterpart yet, so onConnectionsChange's live
    // syncWireToBackend() would just fire a guaranteed-to-fail request per
    // edge; real backend nodes+wiring get created all at once, correctly,
    // by the eventual Run action.
    suppressWireSync = true;
    try {
      (edgesData || []).forEach((e) => {
        const s = idMap.get(e.source);
        const t = idMap.get(e.target);
        if (!s || !t) return;
        const sIdx = findOutputSlotIndex(s, e.source_port || 'out');
        const tIdx = e.type === 'attribute' ? ensureAttributeSlot(t, e.target_port) : findInputSlotIndex(t, e.target_port || 'in');
        if (sIdx < 0 || tIdx < 0) return;
        const link = s.connect(sIdx, t, tIdx);
        if (link) { link.psfKind = e.type || 'data'; link.color = KIND_COLORS[link.psfKind] || KIND_COLORS.data; }
      });
    } finally {
      suppressWireSync = false;
    }
    if (graphcanvas) { graphcanvas.setDirty(true, true); fitView(); }
  }

  // ------------------------------------------------------------------
  // Subgraphs: group/ungroup and view/edit (Request C8/C9/C10)
  //
  // "Group into Subgraph" takes the current multi-selection, cuts it out
  // of the graph into its own workflow YAML file, and replaces it with a
  // single SubgraphNode wired the same way the selection was - using the
  // new input_bridges/output_bridges config (see nodes/subgraph.py) so a
  // group with several boundary wires becomes a subgraph with several
  // named external ports, not just one in/one out. "Ungroup" reverses
  // this exactly. "Open Subgraph" navigates the canvas *into* a
  // SubgraphNode's own embedded workflow for viewing/editing, using
  // LiteGraph's own built-in openSubgraph()/closeSubgraph() graph-stack
  // (see LGraphCanvas.prototype.openSubgraph in the vendored library) so
  // the editor doesn't need to reimplement graph-switching itself.
  // ------------------------------------------------------------------

  function updateSubgraphBar() {
    const bar = document.getElementById('subgraphBar');
    const label = document.getElementById('subgraphBarLabel');
    if (!bar) return;
    const depth = subgraphNavStack.length;
    if (depth === 0) {
      bar.style.display = 'none';
      return;
    }
    bar.style.display = 'flex';
    if (label) {
      const crumb = subgraphNavStack.map((f) => f.node.title || f.node.psfType).join(' › ');
      label.textContent = `Main graph › ${crumb}`;
    }
  }

  // Ensures a SubgraphNode has a real workflow_path on disk to open/save
  // against - a SubgraphNode authored with an inline workflow_yaml string
  // (no file) has nothing GET/POST /subgraph-file can read or write, so
  // the first "Open Subgraph" on one writes its current workflow_yaml out
  // to a new auto-named file under /tmp/psf_subgraphs/ and switches the
  // node over to workflow_path, exactly the way "Save as template"
  // already promotes in-memory state to something reusable. Returns the
  // path, or null if neither workflow_path nor workflow_yaml is set at
  // all (nothing to open).
  async function ensureSubgraphWorkflowPath(node) {
    const props = node.properties || {};
    if (props.workflow_path) return props.workflow_path;
    if (!props.workflow_yaml) return null;
    let parsed;
    try {
      parsed = (await fetchJSON('/parse_yaml', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ yaml_text: props.workflow_yaml }),
      }));
    } catch (e) { parsed = null; }
    const data = (parsed && parsed.ok && parsed.data) ? parsed.data : { nodes: [], edges: [] };
    const path = `/tmp/psf_subgraphs/${node.psfId}.yaml`;
    const res = await fetchJSON('/subgraph-file', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, nodes: data.nodes || [], edges: data.edges || [] }),
    });
    if (res.error) { toast(`Could not open subgraph: ${res.error}`); return null; }
    pushConfig(node, 'workflow_yaml', '');
    pushConfig(node, 'workflow_path', path);
    return path;
  }

  async function openSubgraphNode(node) {
    const path = await ensureSubgraphWorkflowPath(node);
    if (!path) { toast('This subgraph has no workflow to open'); return; }
    const data = await fetchJSON(`/subgraph-file?path=${encodeURIComponent(path)}`);
    if (data.error) { toast(`Could not open subgraph: ${data.error}`); return; }
    const sub = new LGraph();
    sub._subgraph_node = node; // read by LiteGraph's closeSubgraph() to re-select/center on return
    const outerGraph = graph;
    graph = sub;
    loadWorkflowIntoGraph(data.nodes, data.edges);
    addSubgraphBoundaryNodes(node, sub);
    graphcanvas.openSubgraph(sub);
    graph = graphcanvas.graph; // stays === sub; keeps the module-level var authoritative
    subgraphNavStack.push({ path, node, outerGraph });
    updateSubgraphBar();
    toast(`Editing subgraph: ${node.title || node.psfType}`);
  }

  async function closeSubgraphFrame(save) {
    const frame = subgraphNavStack[subgraphNavStack.length - 1];
    if (!frame) return;
    if (save) {
      // Read the edge-point wiring back off the two boundary nodes
      // before stripping them out, and push the result onto the
      // SubgraphNode itself (frame.node, in the *parent* graph) the same
      // way any other config edit is pushed - this is what actually
      // "wires" a subgraph edge point to an inner node/port from inside
      // the subgraph editor (see addSubgraphBoundaryNodes() above for
      // the full story).
      const { inputBridges, outputBridges } = collectBoundaryBridges(graph);
      removeSubgraphBoundaryNodes(graph);
      pushConfig(frame.node, 'input_bridges', inputBridges);
      pushConfig(frame.node, 'output_bridges', outputBridges);
      const wf = graphToWorkflow();
      const res = await fetchJSON('/subgraph-file', {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: frame.path, nodes: wf.nodes, edges: wf.edges }),
      });
      if (res.error) { toast(`Could not save subgraph: ${res.error}`); return; }
    } else {
      removeSubgraphBoundaryNodes(graph);
    }
    graphcanvas.closeSubgraph();
    graph = graphcanvas.graph; // back to the frame's outerGraph
    psfNodesById.clear();
    graph._nodes.forEach((n) => { if (n.psfId) psfNodesById.set(n.psfId, n); });
    subgraphNavStack.pop();
    updateSubgraphBar();
    toast(save ? 'Subgraph saved' : 'Subgraph closed (not saved)');
  }

  // Collapses the current multi-selection into a single new SubgraphNode:
  // any edge with both ends inside the selection becomes an edge in the
  // new subgraph's own workflow file; any edge with exactly one end
  // inside becomes an input/output bridge (see nodes/subgraph.py's
  // input_bridges/output_bridges), giving the new SubgraphNode one
  // external port per distinct boundary (node, port) pair rather than
  // forcing everything through a single in/out like the old
  // single-bridge shape did.
  async function groupSelectedIntoSubgraph() {
    const sel = graphcanvas && graphcanvas.selected_nodes ? Object.values(graphcanvas.selected_nodes) : [];
    const nodes = sel.filter((n) => n.psfId);
    if (nodes.length === 0) { toast('Select one or more nodes first (drag a box, or Ctrl/Shift-click)'); return; }
    const idSet = new Set(nodes.map((n) => n.psfId));
    const wf = graphToWorkflow();
    const innerNodes = wf.nodes.filter((n) => idSet.has(n.id));
    const innerEdgesRaw = [];
    const inboundBoundary = [];  // edges from outside -> inside
    const outboundBoundary = []; // edges from inside -> outside
    wf.edges.forEach((e) => {
      const sIn = idSet.has(e.source), tIn = idSet.has(e.target);
      if (sIn && tIn) innerEdgesRaw.push(e);
      else if (!sIn && tIn) inboundBoundary.push(e);
      else if (sIn && !tIn) outboundBoundary.push(e);
    });

    // Dedupe boundary edges by their *inside* endpoint (node+port) - two
    // external sources feeding the same internal port still only need
    // one external port on the new SubgraphNode.
    const inputBridges = [];
    const inputExternalPortFor = new Map(); // "node|port" -> external port name
    inboundBoundary.forEach((e) => {
      const key = `${e.target}|${e.target_port}`;
      if (!inputExternalPortFor.has(key)) {
        const externalPort = 'in' + inputBridges.length;
        inputExternalPortFor.set(key, externalPort);
        inputBridges.push({ external_port: externalPort, node_id: e.target, port: e.target_port });
      }
    });
    const outputBridges = [];
    const outputExternalPortFor = new Map();
    outboundBoundary.forEach((e) => {
      const key = `${e.source}|${e.source_port}`;
      if (!outputExternalPortFor.has(key)) {
        const externalPort = 'out' + outputBridges.length;
        outputExternalPortFor.set(key, externalPort);
        outputBridges.push({ external_port: externalPort, node_id: e.source, port: e.source_port });
      }
    });

    // Save the extracted nodes/edges as the new subgraph's own workflow
    // file, preserving their relative layout.
    const path = `/tmp/psf_subgraphs/${genId()}.yaml`;
    const saveRes = await fetchJSON('/subgraph-file', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, nodes: innerNodes, edges: innerEdgesRaw }),
    });
    if (saveRes.error) { toast(`Could not create subgraph: ${saveRes.error}`); return; }

    // Centroid of the removed nodes, for where the new SubgraphNode lands.
    const cx = innerNodes.reduce((a, n) => a + (n.x || 0), 0) / innerNodes.length;
    const cy = innerNodes.reduce((a, n) => a + (n.y || 0), 0) / innerNodes.length;

    const sgNode = createPsfNode('SubgraphNode', {
      x: cx, y: cy, label: 'Subgraph',
      config: { workflow_path: path, input_bridges: inputBridges, output_bridges: outputBridges },
    });
    if (!sgNode) { toast('Could not create SubgraphNode (is it registered?)'); return; }

    // Rewire boundary edges onto the new SubgraphNode's external ports,
    // then remove the original nodes (LiteGraph's graph.remove(node) also
    // removes every link attached to it, so the old internal/boundary
    // links are cleaned up for free).
    inboundBoundary.forEach((e) => {
      const externalPort = inputExternalPortFor.get(`${e.target}|${e.target_port}`);
      const srcNode = psfNodesById.get(e.source);
      if (!srcNode) return;
      const sIdx = findOutputSlotIndex(srcNode, e.source_port);
      const tIdx = findInputSlotIndex(sgNode, externalPort);
      if (sIdx >= 0 && tIdx >= 0) {
        const link = srcNode.connect(sIdx, sgNode, tIdx);
        if (link) { link.psfKind = e.type || 'data'; link.color = KIND_COLORS[link.psfKind] || KIND_COLORS.data; }
      }
    });
    outboundBoundary.forEach((e) => {
      const externalPort = outputExternalPortFor.get(`${e.source}|${e.source_port}`);
      const tgtNode = psfNodesById.get(e.target);
      if (!tgtNode) return;
      const sIdx = findOutputSlotIndex(sgNode, externalPort);
      const tIdx = e.type === 'attribute' ? ensureAttributeSlot(tgtNode, e.target_port) : findInputSlotIndex(tgtNode, e.target_port);
      if (sIdx >= 0 && tIdx >= 0) {
        const link = sgNode.connect(sIdx, tgtNode, tIdx);
        if (link) { link.psfKind = e.type || 'data'; link.color = KIND_COLORS[link.psfKind] || KIND_COLORS.data; }
      }
    });
    nodes.forEach((n) => { psfNodesById.delete(n.psfId); graph.remove(n); });
    graphcanvas.selected_nodes = {};
    graphcanvas.setDirty(true, true);
    toast(`Grouped ${nodes.length} node(s) into a subgraph`);
  }

  // Reverses groupSelectedIntoSubgraph(): recreates the SubgraphNode's
  // internal nodes/edges directly in the parent graph (offset so they
  // land near where the SubgraphNode was), rewires its external
  // connections onto the corresponding bridge node/port, and removes the
  // SubgraphNode itself.
  async function ungroupSubgraphNode(sgNode) {
    const path = await ensureSubgraphWorkflowPath(sgNode);
    if (!path) { toast('This subgraph has no workflow to ungroup'); return; }
    const data = await fetchJSON(`/subgraph-file?path=${encodeURIComponent(path)}`);
    if (data.error) { toast(`Could not ungroup: ${data.error}`); return; }

    const props = sgNode.properties || {};
    const inputBridges = props.input_bridges && props.input_bridges.length
      ? props.input_bridges
      : [{ external_port: props.input_port || 'in', node_id: props.input_node_id }];
    const outputBridges = props.output_bridges && props.output_bridges.length
      ? props.output_bridges
      : [{ external_port: props.output_port || 'out', node_id: props.output_node_id }];

    const [ox, oy] = sgNode.pos;
    const idMap = new Map();
    (data.nodes || []).forEach((n) => {
      // syncBackend: false - same reasoning as loadWorkflowIntoGraph()
      // above: this is a bulk restore of a subgraph's saved nodes, not a
      // live single-node add.
      const node = createPsfNode(n.type, {
        config: n.config || {}, label: n.label,
        x: ox + (n.x || 0) - 120, y: oy + (n.y || 0),
        syncBackend: false,
      });
      if (node) idMap.set(n.id, node);
    });
    // suppressWireSync spans both loops below: the freshly-recreated
    // internal nodes have no backend counterpart yet (syncBackend: false
    // above), so every connect() here - including the re-point loop's,
    // which can link one of them to an already-live outside node - would
    // otherwise fire a live /nodes/connect request guaranteed to fail.
    // Same reasoning as loadWorkflowIntoGraph()'s own suppression.
    suppressWireSync = true;
    try {
      (data.edges || []).forEach((e) => {
        const s = idMap.get(e.source), t = idMap.get(e.target);
        if (!s || !t) return;
        const sIdx = findOutputSlotIndex(s, e.source_port || 'out');
        const tIdx = e.type === 'attribute' ? ensureAttributeSlot(t, e.target_port) : findInputSlotIndex(t, e.target_port || 'in');
        if (sIdx >= 0 && tIdx >= 0) {
          const link = s.connect(sIdx, t, tIdx);
          if (link) { link.psfKind = e.type || 'data'; link.color = KIND_COLORS[link.psfKind] || KIND_COLORS.data; }
        }
      });

      // Re-point every wire that used to land on the SubgraphNode itself
      // onto the corresponding freshly-recreated internal node instead.
      Object.values(graph.links || {}).forEach((link) => {
        if (!link) return;
        if (link.target_id === sgNode.id) {
          const externalName = (sgNode.inputs[link.target_slot] || {}).name;
          const bridge = inputBridges.find((b) => b.external_port === externalName);
          const innerNode = bridge && idMap.get(bridge.node_id);
          const srcNode = graph.getNodeById(link.origin_id);
          if (innerNode && srcNode) {
            const tIdx = findInputSlotIndex(innerNode, bridge.port || 'in');
            if (tIdx >= 0) {
              const newLink = srcNode.connect(link.origin_slot, innerNode, tIdx);
              if (newLink) { newLink.psfKind = link.psfKind; newLink.color = link.color; }
            }
          }
        } else if (link.origin_id === sgNode.id) {
          const externalName = (sgNode.outputs[link.origin_slot] || {}).name;
          const bridge = outputBridges.find((b) => b.external_port === externalName);
          const innerNode = bridge && idMap.get(bridge.node_id);
          const tgtNode = graph.getNodeById(link.target_id);
          if (innerNode && tgtNode) {
            const sIdx = findOutputSlotIndex(innerNode, bridge.port || 'out');
            if (sIdx >= 0) {
              const newLink = innerNode.connect(sIdx, tgtNode, link.target_slot);
              if (newLink) { newLink.psfKind = link.psfKind; newLink.color = link.color; }
            }
          }
        }
      });
    } finally {
      suppressWireSync = false;
    }

    psfNodesById.delete(sgNode.psfId);
    graph.remove(sgNode);
    graphcanvas.setDirty(true, true);
    toast('Subgraph ungrouped');
  }

  // ------------------------------------------------------------------
  // Toolbar actions
  // ------------------------------------------------------------------
  async function runWorkflow() {
    try {
      if (currentSessionId) {
        const info = await fetchJSON(`/sessions/${currentSessionId}`);
        if (info && info.status === 'paused') {
          const r = await fetchJSON(`/sessions/${currentSessionId}/resume`, { method: 'POST' });
          if (r.error) { alert('Cannot resume workflow: ' + r.error); }
          return;
        }
        await fetch(`/sessions/${currentSessionId}/stop`, { method: 'POST' }).catch(() => {});
        currentSessionId = null;
      }
      const wf = graphToRunPayload();
      const saveJson = await fetchJSON('/workflows', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(wf) });
      if (saveJson.error) { alert('Cannot run workflow: ' + saveJson.error); return; }
      const sessJson = await fetchJSON('/sessions', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ workflow_path: saveJson.path }) });
      if (sessJson.error) { alert('Cannot run workflow: ' + sessJson.error); return; }
      currentSessionId = sessJson.id;
      const startJson = await fetchJSON(`/sessions/${currentSessionId}/start`, { method: 'POST' });
      if (startJson.error) { alert('Cannot run workflow: ' + startJson.error); }
    } catch (err) {
      alert('Failed to run workflow: ' + err.message);
    }
  }

  async function globalControl(action) {
    if (action === 'run') { await runWorkflow(); return; }
    if (action === 'stop') {
      if (currentSessionId) { await fetch(`/sessions/${currentSessionId}/stop`, { method: 'POST' }).catch(() => {}); currentSessionId = null; }
      return;
    }
    if (action === 'pause') {
      if (currentSessionId) await fetch(`/sessions/${currentSessionId}/pause`, { method: 'POST' }).catch(() => {});
      return;
    }
    if (action === 'step') {
      // Bug fix (found during a codebase-wide audit for exactly this
      // shape of problem): this used to only console.log() each
      // TimerNode's id - it never actually told the backend to do
      // anything, so clicking the global "Step" toolbar button had zero
      // real effect on a running workflow. The per-node right-click
      // "Step" menu item (see PSFNode's context-menu options above)
      // already does this correctly via nodeAction(psfId, 'step') ->
      // POST /nodes/{id}/step, which BaseNode.handle_control() (core/
      // node.py) supports for every node type, not just TimerNode - the
      // TimerNode filter here is kept as-is (this button's own, narrower
      // "advance the clock(s) driving the workflow by one tick" scope,
      // distinct from the generic per-node step already available via
      // the right-click menu), just wired to the real endpoint instead
      // of a fake one.
      const timers = graph._nodes.filter((n) => n.psfType === 'TimerNode');
      if (timers.length === 0) { toast('No TimerNode on the canvas to step'); return; }
      timers.forEach((n) => nodeAction(n.psfId, 'step'));
      toast(`Stepped ${timers.length} timer node${timers.length === 1 ? '' : 's'}`);
    }
  }

  function exportYAML() {
    const wf = graphToWorkflow();
    const yaml = 'nodes:\n' + wf.nodes.map((n) => `  - id: ${n.id}\n    type: ${n.type}\n    label: ${n.label}\n    config: ${JSON.stringify(n.config)}`).join('\n')
      + '\n\nedges:\n' + wf.edges.map((e) => `  - source: ${e.source}\n    target: ${e.target}\n    source_port: ${e.source_port}\n    target_port: ${e.target_port}\n    type: ${e.type}`).join('\n');
    const blob = new Blob([yaml], { type: 'text/yaml' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'workflow.yaml';
    a.click();
  }

  async function importYAML(evt) {
    const file = evt.target.files[0];
    if (!file) return;
    const text = await file.text();
    try {
      const j = await fetchJSON('/parse_yaml', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ yaml_text: text }) });
      if (!j.ok) throw new Error(j.error || 'parse error');
      const obj = j.data || {};
      loadWorkflowIntoGraph(obj.nodes || [], obj.edges || []);
    } catch (err) {
      alert('Invalid YAML: ' + err.message);
    }
  }

  async function loadDemo() {
    try {
      const text = await (await fetch('/example_workflow.yaml')).text();
      const j = await fetchJSON('/parse_yaml', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ yaml_text: text }) });
      if (!j.ok) throw new Error(j.error || 'parse error');
      const obj = j.data || {};
      loadWorkflowIntoGraph(obj.nodes || [], obj.edges || []);
    } catch (err) {
      alert('Failed to load demo: ' + err.message);
    }
  }

  // Bug fix (this was a real, confirmed gap - sessions are fully
  // discoverable and controllable through the HTTP API/CLI/MCP, but not
  // through this editor at all): the old loadSession() called GET
  // /sessions, got the real list back, and then just checked whether it
  // was empty before immediately discarding it and asking the person to
  // type a raw session id into a bare browser prompt() - something they
  // could only know by already having used the CLI or curl. There was no
  // way to actually see what sessions existed, or to start/stop/pause/
  // resume/delete one, from the editor at all. This replaces that with a
  // real picker: a modal listing every session's id, workflow path,
  // status, and node/edge counts, each with its own Load/Start/Stop/
  // Pause/Resume/Delete buttons - the same lifecycle actions already
  // exposed via POST /sessions/{id}/... - so the session list fetched
  // here is actually shown to the person, not just fetched and dropped.
  async function openSessionPicker() {
    const overlay = openModal('Sessions', '<div id="sessionListBody" class="muted">Loading…</div>');
    overlay.querySelector('.modal').classList.add('wide');
    await refreshSessionPicker();
  }

  async function refreshSessionPicker() {
    const body = document.getElementById('sessionListBody');
    if (!body) return; // modal was closed while a request was in flight
    try {
      const j = await fetchJSON('/sessions');
      const sessions = j.sessions || [];
      if (sessions.length === 0) {
        body.innerHTML = '<div class="muted">No sessions yet. Sessions are created by "Run", or via the CLI/API/MCP.</div>';
        return;
      }
      body.innerHTML = sessions.map(sessionRowHtml).join('');
      sessions.forEach((s) => wireSessionRowActions(body, s.id));
    } catch (err) {
      body.innerHTML = `<div class="muted">Failed to load sessions: ${err.message}</div>`;
    }
  }

  function sessionRowHtml(s) {
    const status = (s.status || 'unknown').toLowerCase();
    const label = s.workflow_path ? s.workflow_path.split(/[\\/]/).pop() : '(no workflow path)';
    return `
      <div class="session-row" data-sid="${s.id}">
        <div class="session-row-main">
          <strong title="${s.workflow_path || ''}">${label}</strong>
          <span class="session-status ${status}">${s.status || 'unknown'}</span>
        </div>
        <div class="session-row-meta">id: ${s.id} · ${s.nodes ?? 0} nodes · ${s.edges ?? 0} edges</div>
        <div class="session-row-actions">
          <button data-act="load">Load into canvas</button>
          <button class="secondary" data-act="start">Start</button>
          <button class="secondary" data-act="pause">Pause</button>
          <button class="secondary" data-act="resume">Resume</button>
          <button class="secondary" data-act="stop">Stop</button>
          <button class="ghost" data-act="delete">Delete</button>
        </div>
      </div>`;
  }

  function wireSessionRowActions(container, sessionId) {
    const row = container.querySelector(`.session-row[data-sid="${cssEscape(sessionId)}"]`);
    if (!row) return;
    row.querySelector('[data-act="load"]').onclick = async () => {
      const wf = await fetchJSON('/sessions/' + encodeURIComponent(sessionId) + '/workflow');
      if (wf.error) { toast(wf.error); return; }
      loadWorkflowIntoGraph(wf.nodes || [], wf.edges || []);
      toast('Session loaded into canvas: ' + sessionId);
      closeModal();
    };
    row.querySelector('[data-act="start"]').onclick = async () => {
      await fetch(`/sessions/${sessionId}/start`, { method: 'POST' }).catch(() => {});
      refreshSessionPicker();
    };
    row.querySelector('[data-act="pause"]').onclick = async () => {
      await fetch(`/sessions/${sessionId}/pause`, { method: 'POST' }).catch(() => {});
      refreshSessionPicker();
    };
    row.querySelector('[data-act="resume"]').onclick = async () => {
      await fetch(`/sessions/${sessionId}/resume`, { method: 'POST' }).catch(() => {});
      refreshSessionPicker();
    };
    row.querySelector('[data-act="stop"]').onclick = async () => {
      await fetch(`/sessions/${sessionId}/stop`, { method: 'POST' }).catch(() => {});
      refreshSessionPicker();
    };
    row.querySelector('[data-act="delete"]').onclick = async () => {
      await fetch(`/sessions/${sessionId}`, { method: 'DELETE' }).catch(() => {});
      refreshSessionPicker();
    };
  }

  // Minimal CSS.escape polyfill for the one place this file needs it
  // (building a data-sid attribute selector above) - session ids are
  // uuid4 strings today so this never actually triggers, but a
  // hand-typed/older session id (dashes, anything else CSS.escape would
  // matter for) shouldn't be able to break the selector.
  function cssEscape(s) {
    return window.CSS && CSS.escape ? CSS.escape(s) : String(s).replace(/[^a-zA-Z0-9_-]/g, '\\$&');
  }

  function toggleTriggerMode() {
    state.triggerMode = !state.triggerMode;
    const btn = document.getElementById('triggerModeBtn');
    if (btn) btn.textContent = 'Trigger Wiring: ' + (state.triggerMode ? 'On' : 'Off');
  }
  function toggleEndpointMode() {
    state.endpointMode = !state.endpointMode;
    const btn = document.getElementById('endpointModeBtn');
    if (btn) btn.textContent = 'Endpoint Wiring: ' + (state.endpointMode ? 'On' : 'Off');
  }
  function clearAllEdges() {
    Object.keys(graph.links || {}).forEach((id) => graph.removeLink(Number(id)));
    graphcanvas.setDirty(true, true);
  }
  function zoomBy(factor) {
    graphcanvas.ds.changeScale(graphcanvas.ds.scale * factor, [canvasEl.width / 2, canvasEl.height / 2]);
    graphcanvas.setDirty(true, true);
  }
  function fitView() {
    if (!graph._nodes.length) return;
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    graph._nodes.forEach((n) => {
      minX = Math.min(minX, n.pos[0]);
      minY = Math.min(minY, n.pos[1] - (n.flags && n.flags.collapsed ? 0 : 20));
      maxX = Math.max(maxX, n.pos[0] + n.size[0]);
      maxY = Math.max(maxY, n.pos[1] + n.size[1]);
    });
    const pad = 80;
    const w = canvasEl.width / (window.devicePixelRatio || 1);
    const h = canvasEl.height / (window.devicePixelRatio || 1);
    const cw = (maxX - minX) + pad * 2;
    const ch = (maxY - minY) + pad * 2;
    const scale = Math.min(w / cw, h / ch, 2);
    graphcanvas.ds.scale = scale;
    graphcanvas.ds.offset[0] = -minX + pad + (w / scale - cw) / 2;
    graphcanvas.ds.offset[1] = -minY + pad + (h / scale - ch) / 2;
    graphcanvas.setDirty(true, true);
  }

  // ------------------------------------------------------------------
  // Glow-flash propagation: "add a glowing border effect (just a flash)
  // to nodes which received input or emitted output further down the
  // chain". A node's genuinely-new output (detected in pollNodeStatus()
  // below) flashes that node immediately and schedules the same flash,
  // slightly delayed per hop, on every node wired directly downstream of
  // it - and recursively from there - so the glow visibly travels along
  // the wires the way the data itself does, rather than each node only
  // ever lighting up on its own next poll tick (which, for a fast chain,
  // would make every node appear to flash "at once" on the same ~1s poll
  // rather than in the order data actually moved through them).
  //
  // Depth-capped (not just relying on "no more outgoing links") so a
  // cyclic or very deep/fan-out graph can't schedule an ever-growing
  // storm of setTimeout calls from one upstream change.
  const FLASH_MS = 900;
  const FLASH_HOP_DELAY_MS = 140;
  const FLASH_MAX_DEPTH = 6;
  let flashAnimating = false;
  // LiteGraph's canvas here is event-driven (setDirty() triggers one
  // repaint, not a continuous render loop - see the ~1s setInterval that
  // drives pollNodeStatus() below, the only other regular redraw source)
  // rather than a persistent requestAnimationFrame loop the way the
  // minimap canvas has. onDrawForeground's glow alpha is computed from
  // Date.now(), so without something repainting every frame while a
  // flash is active, the glow would either not visibly fade at all (one
  // static frame, held until the next unrelated redraw wipes it) or, once
  // FLASH_MS elapses between poll ticks, disappear before anything ever
  // redraws it away. This keeps requesting frames - and nothing else -
  // for exactly as long as any flash is still pending, so the fade
  // actually animates smoothly and this stops costing anything once
  // nothing is flashing.
  function ensureFlashAnimation() {
    if (flashAnimating) return;
    flashAnimating = true;
    const step = () => {
      const now = Date.now();
      const stillFlashing = Object.values(nodeFlashUntil).some((t) => t > now);
      if (graphcanvas) graphcanvas.setDirty(true, true);
      if (stillFlashing) {
        requestAnimationFrame(step);
      } else {
        flashAnimating = false;
      }
    };
    requestAnimationFrame(step);
  }
  function flashNode(psfId, atMs) {
    nodeFlashUntil[psfId] = atMs + FLASH_MS;
    ensureFlashAnimation();
  }
  function propagateFlash(node, depth) {
    depth = depth || 0;
    if (!node || depth >= FLASH_MAX_DEPTH) return;
    (node.outputs || []).forEach((out) => {
      (out.links || []).forEach((linkId) => {
        const link = graph.links[linkId];
        const target = link && graph.getNodeById(link.target_id);
        if (!target || !target.psfId) return;
        setTimeout(() => {
          flashNode(target.psfId, Date.now());
          propagateFlash(target, depth + 1);
        }, FLASH_HOP_DELAY_MS * (depth + 1));
      });
    });
  }

  // ------------------------------------------------------------------
  // Status polling -> node color/LED/progress (mirrors old pollNodeStatus)
  // ------------------------------------------------------------------
  async function pollNodeStatus() {
    try {
      const data = await fetchJSON('/nodes');
      const now = Date.now();
      Object.entries(data).forEach(([nid, info]) => {
        const node = psfNodesById.get(nid);
        if (!node) return;
        const h = (info.health || {}).health || 'unknown';
        let led = 'idle';
        if (h === 'error') led = 'error';
        else if (h === 'healthy' || h === 'starting') led = 'active';
        // isBlinking covers two distinct triggers sharing the same small
        // status-dot flicker: nodeBlinkUntil (a deliberate manual-emit
        // click - see manualEmitNode()) and nodeFlashUntil (genuinely new
        // output detected below, or a propagated downstream flash) - it
        // used to be driven by nodeLastSeen/"an item exists at all",
        // which is true forever after a node's first-ever emission (no
        // comparison against what was seen last poll), so the dot never
        // actually stopped blinking once anything had happened even once.
        const isBlinking = now < Math.max(nodeBlinkUntil[nid] || 0, nodeFlashUntil[nid] || 0);
        const color = STATUS_COLORS[led] || STATUS_COLORS.idle;
        node.boxcolor = isBlinking && (Math.floor(now / 300) % 2 === 0) ? '#e2e8f0' : color;
        node.color = led === 'idle' ? undefined : color;
      });
      await Promise.all(graph._nodes.map(async (n) => {
        try {
          // DisplayNode's inline readout/scrollback needs more than the
          // last 1 item everyone else gets polled with, so it fetches its
          // own n=DISPLAY_HISTORY_LINES separately below instead of
          // reusing this shared n=1 fetch.
          const j = n.psfType === 'DisplayNode'
            ? await fetchJSON(`/nodes/${n.psfId}/last?n=${DISPLAY_HISTORY_LINES}`)
            : await fetchJSON(`/nodes/${n.psfId}/last?n=1`);
          const lastEntry = j.last && j.last.length ? j.last[j.last.length - 1] : null;
          if (lastEntry) {
            nodeLastSeen[n.psfId] = now;
            // JSON.stringify as a cheap "did this actually change since
            // the last poll" signature - see nodeLastItemSig's own
            // comment above for why a plain "an item exists" check (the
            // old behavior) isn't enough to make this a one-off flash.
            const sig = JSON.stringify(lastEntry);
            if (nodeLastItemSig[n.psfId] !== sig) {
              nodeLastItemSig[n.psfId] = sig;
              flashNode(n.psfId, now);
              propagateFlash(n, 0);
            }
          }
          if (n.psfType === 'ListStringsNode') {
            const cfgRes = await fetchJSON(`/nodes/${n.psfId}/config`);
            let arr = (cfgRes.config || {}).strings || [];
            if (typeof arr === 'string') arr = arr.split(',').map((s) => s.trim());
            const total = arr.length || 1;
            let idx = 0;
            if (j.last && j.last.length) {
              const lastItem = j.last[j.last.length - 1].item;
              if (lastItem && typeof lastItem === 'object' && 'index' in lastItem) idx = lastItem.index;
            }
            nodeProgressMap.set(n.psfId, Math.min(100, Math.round(((idx % total) / total) * 100)));
          }
          if (n.psfType === 'DisplayNode' && n._liveViewWidget) {
            // Newest first: DisplayNode emits {'display': msg, 'item':
            // item} on 'out' (nodes/display.py), so each /last entry's
            // .item.display is already the exact "prefix+item+suffix"
            // string this node prints server-side - reuse it verbatim
            // rather than re-deriving it here, so the canvas readout can
            // never drift out of sync with what the node actually did.
            // Feeds the dedicated multi-line "live view" panel drawn
            // above the config widgets (see addWidgetsForNode()'s
            // psf_live_view widget) as a plain array of raw display
            // strings - word-wrapping happens in that widget's own draw()
            // call, where a real canvas context is available to measure
            // text against - graphcanvas.setDirty() below repaints it on
            // every poll tick, same as everything else here.
            n._liveViewEntries = (j.last || []).slice().reverse().map((entry) => (
              entry && entry.item && typeof entry.item === 'object' && 'display' in entry.item
                ? String(entry.item.display) : (entry ? String(entry.item) : '')
            ));
          }
        } catch (e) { /* ignore per-node polling errors */ }
      }));
      if (graphcanvas) graphcanvas.setDirty(true, true);
      // Feedback: "live view is flickering and the scrollbar is
      // unusable" - this used to call refreshDetailsPanel(), which
      // rebuilds the *entire* details panel's innerHTML (title,
      // description, buttons, and the live-view <pre> together) every
      // single tick (every 1s - see the setInterval below), tearing down
      // and recreating the <pre> element itself each time. That reset any
      // manual scroll position inside it to 0 before a person could ever
      // read down the list, and visibly re-flashed the whole panel even
      // though only the live-view content could possibly have changed.
      // loadLive() touches only the <pre>'s own content, preserves scroll
      // position across the update, and skips the DOM write entirely when
      // nothing actually changed - see its own comment for the details.
      if (selectedNode) loadLive(selectedNode);
    } catch (e) { /* backend not reachable this tick */ }
  }

  // ------------------------------------------------------------------
  // Minimap - litegraph.js core has no built-in one, so this is a small
  // independent overlay canvas kept in sync with graphcanvas.ds.
  // ------------------------------------------------------------------
  function initMinimap() {
    const mini = document.getElementById('minimap');
    if (!mini) return;
    const mctx = mini.getContext('2d');
    function draw() {
      const w = mini.width, h = mini.height;
      mctx.fillStyle = '#0f172a';
      mctx.fillRect(0, 0, w, h);
      if (!graph._nodes.length) { requestAnimationFrame(draw); return; }
      let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
      graph._nodes.forEach((n) => {
        minX = Math.min(minX, n.pos[0]); minY = Math.min(minY, n.pos[1]);
        maxX = Math.max(maxX, n.pos[0] + n.size[0]); maxY = Math.max(maxY, n.pos[1] + n.size[1]);
      });
      const vw = Math.max(1, canvasEl.width / (window.devicePixelRatio || 1) / graphcanvas.ds.scale);
      const vh = Math.max(1, canvasEl.height / (window.devicePixelRatio || 1) / graphcanvas.ds.scale);
      const vx = -graphcanvas.ds.offset[0], vy = -graphcanvas.ds.offset[1];
      minX = Math.min(minX, vx); minY = Math.min(minY, vy);
      maxX = Math.max(maxX, vx + vw); maxY = Math.max(maxY, vy + vh);
      const pad = 40;
      minX -= pad; minY -= pad; maxX += pad; maxY += pad;
      const worldW = Math.max(1, maxX - minX), worldH = Math.max(1, maxY - minY);
      const s = Math.min(w / worldW, h / worldH);
      const ox = (w - worldW * s) / 2, oy = (h - worldH * s) / 2;
      const toMini = (x, y) => [ox + (x - minX) * s, oy + (y - minY) * s];
      graph._nodes.forEach((n) => {
        const [x, y] = toMini(n.pos[0], n.pos[1]);
        mctx.fillStyle = n.color || '#3b82f6';
        mctx.fillRect(x, y, Math.max(2, n.size[0] * s), Math.max(2, n.size[1] * s));
      });
      const [vx0, vy0] = toMini(vx, vy);
      mctx.strokeStyle = '#facc15';
      mctx.lineWidth = 1.5;
      mctx.strokeRect(vx0, vy0, vw * s, vh * s);
      mini._proj = { minX, minY, s, ox, oy };
      requestAnimationFrame(draw);
    }
    requestAnimationFrame(draw);
    mini.addEventListener('mousedown', (e) => {
      const proj = mini._proj;
      if (!proj) return;
      const rect = mini.getBoundingClientRect();
      const mx = (e.clientX - rect.left) * (mini.width / rect.width);
      const my = (e.clientY - rect.top) * (mini.height / rect.height);
      const worldX = (mx - proj.ox) / proj.s + proj.minX;
      const worldY = (my - proj.oy) / proj.s + proj.minY;
      const w = canvasEl.width / (window.devicePixelRatio || 1);
      const h = canvasEl.height / (window.devicePixelRatio || 1);
      graphcanvas.ds.offset[0] = -worldX + w / (2 * graphcanvas.ds.scale);
      graphcanvas.ds.offset[1] = -worldY + h / (2 * graphcanvas.ds.scale);
      graphcanvas.setDirty(true, true);
    });
  }

  // ------------------------------------------------------------------
  // Media previews in the live view (media plan phase 6a).
  //
  // The backend's /nodes/{id}/last runs every item through
  // core/media.py's to_jsonable(), which turns a MediaItem into a
  // summary object: {"$media": kind, mime, size, ref, meta, preview_url}.
  // preview_url points at GET /media/{ref}, which - like every other
  // non-exempt route - needs the API key. An <img src> can't send an
  // Authorization header, so each preview is fetched through the
  // auth-injecting window.fetch wrapper at the top of this file and shown
  // from a blob: object URL instead.
  // ------------------------------------------------------------------
  const MEDIA_URL_CACHE_MAX = 32;
  const MEDIA_GALLERY_MAX = 12;
  // preview_url -> Promise<objectURL|null>, oldest first (Map keeps
  // insertion order; a hit is re-inserted to mark it recently used).
  const mediaUrlCache = new Map();

  function findMediaItems(value, out, seen, depth) {
    out = out || [];
    seen = seen || new Set();
    depth = depth || 0;
    if (!value || typeof value !== 'object' || depth > 6) return out;
    if (typeof value.$media === 'string' && value.preview_url) {
      if (!seen.has(value.ref)) { seen.add(value.ref); out.push(value); }
      return out;
    }
    const children = Array.isArray(value) ? value : Object.values(value);
    children.forEach((v) => findMediaItems(v, out, seen, depth + 1));
    return out;
  }

  function fetchMediaUrl(previewUrl) {
    if (mediaUrlCache.has(previewUrl)) {
      const hit = mediaUrlCache.get(previewUrl);
      mediaUrlCache.delete(previewUrl);
      mediaUrlCache.set(previewUrl, hit);
      return hit;
    }
    const p = fetch(previewUrl)
      .then((res) => (res.ok ? res.blob() : null))
      .then((blob) => (blob ? URL.createObjectURL(blob) : null))
      .catch(() => null);
    mediaUrlCache.set(previewUrl, p);
    while (mediaUrlCache.size > MEDIA_URL_CACHE_MAX) {
      const [oldKey, oldUrl] = mediaUrlCache.entries().next().value;
      mediaUrlCache.delete(oldKey);
      oldUrl.then((u) => { if (u) URL.revokeObjectURL(u); });
    }
    return p;
  }

  function formatBytes(n) {
    if (n == null) return '';
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
    return `${(n / (1024 * 1024)).toFixed(1)} MB`;
  }

  function mediaCaption(m) {
    const meta = m.meta || {};
    const parts = [m.$media, m.mime];
    if (meta.width && meta.height) parts.push(`${meta.width}×${meta.height}`);
    if (meta.duration != null) parts.push(`${Number(meta.duration).toFixed(2)} s`);
    if (meta.pts != null) parts.push(`pts ${meta.pts}`);
    parts.push(formatBytes(m.size));
    return parts.filter(Boolean).join(' · ');
  }

  function mediaElementTag(m) {
    const major = String(m.mime || '').split('/')[0];
    if (major === 'image') return 'img';
    if (major === 'audio' || m.$media === 'audio' || m.$media === 'audio_chunk') return 'audio';
    if (major === 'video' || m.$media === 'video') return 'video';
    return null;
  }

  // Render `media` (one summary object, or null to hide) into `container`.
  // Re-rendering the same ref is a no-op, so a poll tick never restarts a
  // playing <audio>/<video>. With opts.follow (the default, used by the
  // sidebar), a video_frame/audio_chunk stream shows only its latest item
  // - "last frame" mode, refreshed by the regular 1 s poll - with a pause
  // button to freeze the current frame.
  function renderMediaPreview(container, media, opts) {
    const follow = !opts || opts.follow !== false;
    if (!media) {
      container.hidden = true;
      container.replaceChildren();
      delete container.dataset.ref;
      return;
    }
    container.hidden = false;
    if (container.dataset.ref === media.ref) return;
    if (follow && container.dataset.paused === '1' && container.dataset.ref) return;
    container.dataset.ref = media.ref;

    const tag = mediaElementTag(media);
    const stream = media.$media === 'video_frame' || media.$media === 'audio_chunk';
    const existing = container.querySelector('[data-role="media"]');
    let el = existing && existing.tagName.toLowerCase() === tag && tag === 'img' ? existing : null;
    if (!el) {
      container.replaceChildren();
      if (tag) {
        el = document.createElement(tag);
        el.dataset.role = 'media';
        if (tag !== 'img') el.controls = true;
        if (tag === 'img') el.alt = mediaCaption(media);
        container.appendChild(el);
      } else {
        const link = document.createElement('a');
        link.dataset.role = 'media';
        link.textContent = 'Download';
        link.href = '#';
        link.onclick = (e) => {
          e.preventDefault();
          fetchMediaUrl(media.preview_url).then((u) => { if (u) window.open(u, '_blank', 'noopener'); });
        };
        container.appendChild(link);
      }
      const cap = document.createElement('div');
      cap.className = 'media-cap';
      container.appendChild(cap);
      if (follow && stream) {
        const btn = document.createElement('button');
        btn.className = 'secondary media-pause';
        btn.type = 'button';
        const sync = () => { btn.textContent = container.dataset.paused === '1' ? '▶ Follow live' : '⏸ Pause'; };
        btn.onclick = () => { container.dataset.paused = container.dataset.paused === '1' ? '0' : '1'; sync(); };
        sync();
        container.appendChild(btn);
      }
    }
    const cap = container.querySelector('.media-cap');
    if (cap) cap.textContent = (stream && follow ? 'live · ' : '') + mediaCaption(media);
    if (tag) {
      const ref = media.ref;
      fetchMediaUrl(media.preview_url).then((u) => {
        if (container.dataset.ref !== ref) return; // a newer item arrived meanwhile
        if (u) { el.src = u; container.classList.remove('expired'); }
        else { container.classList.add('expired'); if (cap) cap.textContent = `${mediaCaption(media)} · expired`; }
      });
    }
  }

  // ------------------------------------------------------------------
  // Details / advanced panel (right sidebar) - the one surviving
  // Inspector role per the chosen scope: advanced raw-JSON edit + live
  // status, everything else now lives inline on the node as widgets.
  // ------------------------------------------------------------------
  let selectedNode = null;
  function refreshDetailsPanel() {
    const panel = document.getElementById('details');
    if (!panel) return;
    if (!selectedNode) { panel.innerHTML = '<p class="muted">No node selected. Click a node to see live status and advanced JSON here.</p>'; return; }
    const n = selectedNode;
    const doc = nodeDocs[n.psfType] || {};
    panel.innerHTML = `
      <h4>${n.title || n.psfType}</h4>
      <div class="muted">${n.psfType} · id ${n.psfId}</div>
      ${doc.desc ? `<p class="muted">${doc.desc}</p>` : ''}
      <button class="secondary" data-act="live">Refresh live view</button>
      <button class="secondary" data-act="json">Advanced JSON…</button>
      <div id="liveMedia" class="live-media" hidden></div>
      <pre id="liveView">loading…</pre>
    `;
    panel.querySelector('[data-act="live"]').onclick = () => loadLive(n);
    panel.querySelector('[data-act="json"]').onclick = () => openAdvancedModal(n);
    loadLive(n);
  }
  function loadLive(node) {
    fetchJSON(`/nodes/${node.psfId}/last?n=50`).then((j) => {
      const el = document.getElementById('liveView');
      if (!el) return;
      // Media plan phase 6a: the newest image/audio/video item in this
      // node's history gets a real preview above the JSON text.
      const mediaEl = document.getElementById('liveMedia');
      if (mediaEl) {
        const media = findMediaItems((j.last || []).slice().reverse());
        renderMediaPreview(mediaEl, media.length ? media[0] : null);
      }
      // Feedback: "the sorting of the messages should be reversed (newest
      // on top)" - /nodes/{id}/last (BaseNode.get_last()) returns oldest-
      // to-newest, the order it's appended in; reverse it here, the same
      // convention DisplayNode's own inline live-view panel already uses
      // for the identical reason (see pollNodeStatus()'s
      // `n._liveViewEntries` handling above).
      const text = JSON.stringify((j.last || []).slice().reverse(), null, 2);
      if (el.textContent === text) return; // nothing changed - skip the write entirely, so a poll tick with no new data can't disturb scroll at all
      // Feedback: "live view (the always visible one) is flickering and
      // the scrollbar is unusable" - this panel used to get its whole
      // *parent* torn down and rebuilt (innerHTML replaced wholesale) once
      // a second by the polling loop below, which reset any manual scroll
      // position to 0 before a person could ever read down the list, and
      // visibly re-flashed the title/buttons around it too even though
      // neither had changed. pollNodeStatus() now calls this function
      // directly instead (see its own comment), so only this `<pre>`'s
      // content is ever touched on a poll tick - and its scroll position
      // is explicitly preserved across that content swap: stay pinned to
      // the top if already there (so "follow live updates" keeps working
      // now that newest is on top), otherwise anchor by distance from the
      // bottom, which survives new items being prepended above whatever
      // a person is currently reading.
      const wasAtTop = el.scrollTop <= 1;
      const distanceFromBottom = el.scrollHeight - el.scrollTop;
      el.textContent = text;
      el.scrollTop = wasAtTop ? 0 : Math.max(0, el.scrollHeight - distanceFromBottom);
    }).catch(() => {
      const el = document.getElementById('liveView');
      if (el) el.textContent = 'error';
    });
  }

  // ------------------------------------------------------------------
  // Small modal helper (advanced JSON editor, live-view popup)
  // ------------------------------------------------------------------
  function openModal(title, bodyHtml) {
    const overlay = document.getElementById('modalOverlay');
    overlay.innerHTML = `<div class="modal"><div class="modal-title">${title}<button class="modal-close">×</button></div><div class="modal-body">${bodyHtml}</div></div>`;
    overlay.classList.add('open');
    overlay.querySelector('.modal-close').onclick = closeModal;
    overlay.onclick = (e) => { if (e.target === overlay) closeModal(); };
    return overlay;
  }
  function closeModal() {
    const overlay = document.getElementById('modalOverlay');
    overlay.classList.remove('open');
    overlay.innerHTML = '';
  }
  function openAdvancedModal(node) {
    const overlay = openModal(`Advanced JSON — ${node.title || node.psfType}`, `
      <textarea id="advJson" rows="14">${JSON.stringify(node.properties || {}, null, 2)}</textarea>
      <div class="modal-actions">
        <button id="advApply">Apply</button>
        <button class="secondary" id="advCancel">Cancel</button>
      </div>
    `);
    overlay.querySelector('#advCancel').onclick = closeModal;
    overlay.querySelector('#advApply').onclick = () => {
      let parsed;
      try { parsed = JSON.parse(overlay.querySelector('#advJson').value); } catch (e) { alert('Invalid JSON'); return; }
      node.properties = parsed;
      fetch(`/nodes/${node.psfId}/config`, { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ config: parsed }) }).catch(() => {});
      addWidgetsForNode(node);
      syncAttributeSlots(node);
      graphcanvas.setDirty(true, true);
      if (selectedNode === node) refreshDetailsPanel();
      closeModal();
    };
  }
  function openLiveModal(node) {
    const overlay = openModal(`Live view — ${node.title || node.psfType}`,
      '<div id="liveModalMedia" class="media-gallery" hidden></div><pre id="liveModalPre">loading…</pre>');
    fetchJSON(`/nodes/${node.psfId}/last?n=50`).then((j) => {
      const el = overlay.querySelector('#liveModalPre');
      // Same newest-first ordering as the sidebar's loadLive() above, for
      // consistency between the two live-view surfaces.
      const entries = (j.last || []).slice().reverse();
      if (el) el.textContent = JSON.stringify(entries, null, 2);
      // Media plan phase 6a: a gallery of the newest distinct media items.
      const gallery = overlay.querySelector('#liveModalMedia');
      const media = findMediaItems(entries).slice(0, MEDIA_GALLERY_MAX);
      if (gallery && media.length) {
        gallery.hidden = false;
        overlay.querySelector('.modal').classList.add('wide');
        media.forEach((m) => {
          const cell = document.createElement('div');
          cell.className = 'live-media';
          gallery.appendChild(cell);
          renderMediaPreview(cell, m, { follow: false });
        });
      }
    });
  }
  function toast(msg) {
    const el = document.getElementById('toast');
    if (!el) { alert(msg); return; }
    el.textContent = msg;
    el.classList.add('show');
    clearTimeout(toast._t);
    toast._t = setTimeout(() => el.classList.remove('show'), 2200);
  }

  // ------------------------------------------------------------------
  // Palette sidebar (grouped buttons, same categories as before) - kept
  // alongside the new double-click/right-click search box rather than
  // replaced by it, so both fast paths (search) and browse (palette)
  // work.
  // ------------------------------------------------------------------
  function buildPalette() {
    const palette = document.getElementById('palette');
    if (!palette) return;
    palette.innerHTML = '';
    Object.entries(nodeGroups).forEach(([group, keys]) => {
      const h = document.createElement('h4');
      h.textContent = group;
      palette.appendChild(h);
      keys.forEach((k) => {
        const meta = nodeTypes[k];
        if (!meta) return;
        const b = document.createElement('button');
        const doc = nodeDocs[meta.type] || {};
        b.textContent = `${meta.icon} ${meta.label}`;
        b.title = doc.desc ? `${doc.desc}` : meta.label;
        const missing = unavailableTypes[meta.type];
        if (missing) {
          // Media plan phase 3: greyed out, with the install hint, rather
          // than creating a node the backend can't instantiate.
          b.classList.add('unavailable');
          b.title = `Not available on this server - ${missing}`;
          b.onclick = () => toast(`${meta.label}: ${missing}`);
          palette.appendChild(b);
          return;
        }
        b.onclick = () => { createPsfNode(k); graphcanvas.setDirty(true, true); };
        palette.appendChild(b);
      });
    });
  }

  function buildToolsList() {
    fetch('/mcp/tools').then((r) => r.json()).then((d) => {
      const ul = document.getElementById('tools');
      if (!ul) return;
      (d.tools || []).forEach((t) => {
        const li = document.createElement('li');
        li.textContent = t.name;
        ul.appendChild(li);
      });
    }).catch(() => {});
  }

  // ------------------------------------------------------------------
  // LiteGraph theme + link rendering customization
  // ------------------------------------------------------------------
  function themeLiteGraph() {
    LiteGraph.NODE_TITLE_COLOR = '#e2e8f0';
    LiteGraph.NODE_SELECTED_TITLE_COLOR = '#facc15';
    LiteGraph.NODE_DEFAULT_COLOR = '#3b82f6';
    LiteGraph.NODE_DEFAULT_BGCOLOR = '#1e293b';
    LiteGraph.NODE_DEFAULT_BOXCOLOR = '#475569';
    LiteGraph.WIDGET_BGCOLOR = '#0f172a';
    LiteGraph.WIDGET_OUTLINE_COLOR = '#334155';
    LiteGraph.WIDGET_TEXT_COLOR = '#e2e8f0';
    LiteGraph.LINK_COLOR = KIND_COLORS.data;
    LiteGraph.EVENT_LINK_COLOR = KIND_COLORS.control;
    LiteGraph.CONNECTING_LINK_COLOR = '#facc15';

    // Dash pattern per edge "kind" - litegraph.js core never touches
    // ctx.setLineDash itself (verified against the vendored source), so
    // this wrap is a safe, purely additive customization: it can't
    // corrupt any dash state the library relies on because there isn't
    // any. Color is *not* handled here - renderLink already reads
    // link.color directly (see connect()'s override below), which takes
        // priority over LGraphCanvas.link_type_colors[link.type].
    const origRenderLink = LGraphCanvas.prototype.renderLink;
    LGraphCanvas.prototype.renderLink = function (ctx, a, b, link) {
      ctx.save();
      const dash = (link && KIND_DASH[link.psfKind]) || [];
      ctx.setLineDash(dash);
      origRenderLink.apply(this, arguments);
      ctx.restore();
    };
  }

  // ------------------------------------------------------------------
  // Init
  // ------------------------------------------------------------------
  async function init() {
    themeLiteGraph();
    try {
      const [ps, cs, av] = await Promise.all([
        fetchJSON('/node-schema').catch(() => ({})),
        fetchJSON('/config-schema').catch(() => ({})),
        fetchJSON('/node-availability').catch(() => ({})),
      ]);
      portSchema = ps || {};
      configSchema = cs || {};
      unavailableTypes = (av && av.unavailable) || {};
    } catch (e) { /* fall back to defaults baked into rebuildPorts */ }

    buildNodeClasses();

    graph = new LGraph();
    // Only hooked on this, the single main outer LGraph instance - never
    // on `sub`, the temporary LGraph opened by openSubgraphNode() to edit
    // a subgraph's insides, since sub's nodes are always created with
    // syncBackend:false and have no real backend counterpart to delete.
    // See syncNodeDeleteToBackend()'s own comment above for the full bug
    // this closes.
    graph.onNodeRemoved = (node) => syncNodeDeleteToBackend(node);
    canvasEl = document.getElementById('graphCanvas');
    // Note: this used to also register a custom 'wheel'/'mousewheel'/
    // 'DOMMouseScroll' listener here, ahead of `new LGraphCanvas(...)`
    // below binding its own, so a wheel event over a DisplayNode's live
    // view would scroll the panel instead of zooming the graph. Removed -
    // see the psf_live_view widget's draw() comment in addWidgetsForNode()
    // for why (resizing a DisplayNode big made that panel swallow zoom
    // over most of the node). A plain wheel here now always reaches
    // litegraph's own zoom handler unconditionally.
    graphcanvas = new LGraphCanvas('#graphCanvas', graph);
    graphcanvas.clear_background_color = '#0d1526';
    graphcanvas.render_canvas_border = false;
    graphcanvas.allow_searchbox = true;
    graphcanvas.render_link_tooltip = false;

    graphcanvas.onNodeSelected = (node) => { selectedNode = node; refreshDetailsPanel(); };
    graphcanvas.onNodeDeselected = () => { selectedNode = null; refreshDetailsPanel(); };

    // The built-in search box (double-click canvas, or right-click ->
    // "Add Node") lists registered node types by their raw type string
    // (e.g. "psf/Inputs/WebInput") - litegraph.js's showSearchBox() has
    // no hook to customize that label, so this purely-cosmetic
    // MutationObserver relabels each result to the catalog's icon+label
    // once it's in the DOM. It only ever rewrites visible text, never
    // `dataset.type` (which the library's own click handler reads to
    // know what to create), so this can't affect which node actually
    // gets added.
    const searchLabelMap = {};
    Object.entries(nodeTypes).forEach(([key, meta]) => { searchLabelMap[fullTypeForKey(key)] = `${meta.icon} ${meta.label}`; });
    new MutationObserver((mutations) => {
      mutations.forEach((m) => m.addedNodes.forEach((el) => {
        if (el.nodeType !== 1) return;
        const items = el.classList && el.classList.contains('lite-search-item') ? [el] : (el.querySelectorAll ? Array.from(el.querySelectorAll('.lite-search-item')) : []);
        items.forEach((item) => {
          const label = searchLabelMap[item.innerText];
          if (label) item.innerText = label;
        });
      }));
    }).observe(document.body, { childList: true, subtree: true });

    // Wire deletion: LiteGraph already removes links when you click one
    // and press Delete/Backspace, or drag from a slot with an existing
    // single link. No extra wiring needed beyond the defaults.

    // Deliberately not calling graph.start(): that spins up litegraph's
    // own per-frame "execute every node's onExecute()" loop, which is
    // its model for graphs it runs itself. pystreamflow's actual
    // execution is the real backend Engine/Session (see runWorkflow()
    // below) - none of these node classes implement onExecute, so that
    // loop would do nothing but spend a requestAnimationFrame every
    // frame forever. The canvas still renders/redraws on its own via
    // LGraphCanvas's normal dirty-driven render loop.
    // Exposed for debugging/support (browser devtools), same spirit as
    // e.g. ComfyUI's `window.app` - not required for normal operation.
    window.__psf = {
      graph, graphcanvas, psfNodesById, state, nodeFlashUntil,
      openSubgraphNode, closeSubgraphFrame, groupSelectedIntoSubgraph, ungroupSubgraphNode,
      // createPsfNode/graphToWorkflow/loadWorkflowIntoGraph added for
      // Phase 4 (wire-kind-unification design) live verification via a
      // real headless-browser session - same "debugging/support" spirit
      // as every other entry here, not testing-only: they're the same
      // real functions a real palette click / Export / Load Session
      // already call, just reachable directly for a script that wants to
      // drive them without simulating a file download or DOM click.
      createPsfNode, graphToWorkflow, loadWorkflowIntoGraph,
    };

    buildPalette();
    buildToolsList();
    initMinimap();
    refreshDetailsPanel();

    // litegraph.js core only auto-resizes the canvas backing store on
    // mousemove (see LGraphCanvas.prototype.processMouseMove) - too late
    // for the very first frame, and it never fires at all for a resize
    // caused by toggling the palette/details asides rather than mouse
    // movement over the canvas itself. A ResizeObserver on the wrapper
    // covers the window-resize case, the initial layout, and both aside
    // toggles (whose CSS width transition keeps firing observer
    // callbacks as it animates) with one mechanism.
    const wrap = document.getElementById('canvasWrap');
    graphcanvas.resize();
    if (window.ResizeObserver) {
      new ResizeObserver(() => graphcanvas.resize()).observe(wrap);
    } else {
      window.addEventListener('resize', () => graphcanvas.resize());
    }

    // Header toolbar
    const on = (id, fn) => { const el = document.getElementById(id); if (el) el.onclick = fn; };
    on('btnRun', () => globalControl('run'));
    on('btnStop', () => globalControl('stop'));
    on('btnPause', () => globalControl('pause'));
    on('btnStep', () => globalControl('step'));
    on('btnZoomIn', () => zoomBy(1.2));
    on('btnZoomOut', () => zoomBy(1 / 1.2));
    on('btnFit', fitView);
    on('btnExport', exportYAML);
    on('btnImportTrigger', () => document.getElementById('importFile').click());
    on('btnDemo', loadDemo);
    on('btnLoadSession', openSessionPicker);
    on('triggerModeBtn', toggleTriggerMode);
    on('endpointModeBtn', toggleEndpointMode);
    on('btnClearWires', clearAllEdges);
    on('btnGroupSubgraph', groupSelectedIntoSubgraph);
    on('btnSubgraphSave', () => closeSubgraphFrame(true));
    on('btnSubgraphDiscard', () => closeSubgraphFrame(false));
    on('btnTogglePalette', () => document.getElementById('paletteAside').classList.toggle('collapsed'));
    on('btnToggleDetails', () => document.getElementById('detailsAside').classList.toggle('collapsed'));
    const importFile = document.getElementById('importFile');
    if (importFile) importFile.onchange = importYAML;

    setInterval(pollNodeStatus, 1000);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
