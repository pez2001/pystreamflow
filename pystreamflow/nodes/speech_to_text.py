import asyncio
import io
import os
import threading
import time
from typing import Any

import httpx

from ..core.media import MediaItem, extension_for_mime
from ..core.node import BaseNode

_LOCAL_MODELS: dict[tuple, Any] = {}
_LOCAL_MODELS_LOCK = threading.Lock()


def _load_local_model(model: str, device: str, compute_type: str):
    """faster-whisper model, cached per (model, device, compute_type) for
    the whole process - loading one takes seconds and lots of memory."""
    key = (model, device, compute_type)
    with _LOCAL_MODELS_LOCK:
        if key not in _LOCAL_MODELS:
            from faster_whisper import WhisperModel

            _LOCAL_MODELS[key] = WhisperModel(model, device=device, compute_type=compute_type)
        return _LOCAL_MODELS[key]


class SpeechToTextNode(BaseNode):
    """Speech recognition (media plan phase 4): an audio (or video) item in,
    its transcript as text out - so the whole text-node family can take it
    from there.

    Two backends, chosen per node with ``backend``:

    - ``api`` (default): POSTs the audio to an OpenAI-compatible
      ``<base_url>/audio/transcriptions`` endpoint - OpenAI itself, or a
      local server such as speaches/faster-whisper-server or LocalAI
      (LM Studio has no transcription endpoint). No extra Python
      dependency. Config: ``base_url`` (default
      ``http://localhost:8000/v1``), ``model`` (default ``whisper-1``),
      ``api_key``, ``timeout`` (read timeout, default 300 s).
    - ``local``: runs faster-whisper in this process
      (``pip install 'pystreamflow[stt]'``; a GPU helps a lot). Config:
      ``model`` (a size like ``small``/``medium``/``large-v3``, or a path;
      default ``small``), ``device`` (``auto``/``cpu``/``cuda``),
      ``compute_type`` (default ``default``), ``beam_size`` (5). The model
      is loaded once per process and shared by all nodes using it.

    Both: ``language`` (e.g. ``de``; empty = auto-detect) and ``prompt``
    (vocabulary/context hint for the model).

    Output ports: ``out`` - the transcript text; ``details`` - ``{text,
    language, duration, segments, pts, source}`` (``segments`` with
    start/end/text where the backend provides them); ``errors`` - the
    error message when a request fails. Chunks are transcribed one at a
    time; feed segments from AudioSegmentNode rather than tiny chunks.
    """

    ACCEPT_KINDS = ("audio", "audio_chunk", "video")

    async def init(self):
        c = self.config
        self.backend = str(c.get('backend', 'api')).lower()
        if self.backend not in ('api', 'local'):
            raise ValueError("SpeechToTextNode: backend must be 'api' or 'local'")
        self.language = (c.get('language') or '').strip() or None
        self.prompt = (c.get('prompt') or '').strip() or None
        if self.backend == 'api':
            self.base_url = str(c.get('base_url') or 'http://localhost:8000/v1').rstrip('/')
            self.model = c.get('model') or 'whisper-1'
            self.api_key = c.get('api_key') or ''
            self.timeout = float(c.get('timeout', 300.0))
        else:
            try:
                import faster_whisper  # noqa: F401
            except ImportError:
                raise ValueError(
                    "SpeechToTextNode backend 'local' needs faster-whisper: pip install 'pystreamflow[stt]'"
                ) from None
            self.model = c.get('model') or 'small'
            self.device = c.get('device') or 'auto'
            self.compute_type = c.get('compute_type') or 'default'
            self.beam_size = int(c.get('beam_size', 5))

    async def process(self):
        client = None
        if self.backend == 'api':
            client = httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=self.timeout))
        try:
            while self._running:
                pipe = self.inputs.get('in')
                if not pipe:
                    await asyncio.sleep(0.5)
                    continue
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                await self.handle(item, client)
        finally:
            if client is not None:
                await client.aclose()

    async def handle(self, item: Any, client: httpx.AsyncClient | None = None) -> dict | None:
        if not (isinstance(item, MediaItem) and item.kind in self.ACCEPT_KINDS):
            self._fail(f"expected an audio item, got {type(item).__name__}")
            return None
        start = time.monotonic()
        try:
            if self.backend == 'api':
                own_client = client is None
                client = client or httpx.AsyncClient(timeout=httpx.Timeout(10.0, read=self.timeout))
                try:
                    result = await self._transcribe_api(item, client)
                finally:
                    if own_client:
                        await client.aclose()
            else:
                result = await asyncio.to_thread(self._transcribe_local, item)
        except Exception as e:
            self._fail(f"{type(e).__name__}: {e}")
            return None
        text = (result.get('text') or '').strip()
        details = {
            'text': text,
            'language': result.get('language'),
            'duration': result.get('duration', item.meta.get('duration')),
            'segments': result.get('segments') or [],
            'pts': item.meta.get('pts'),
            'source': {k: item.meta[k] for k in ('filename', 'source_path', 'index') if k in item.meta},
            'latency_s': round(time.monotonic() - start, 3),
        }
        self.emit('out', text)
        self.emit('details', details)
        return details

    def _fail(self, message: str) -> None:
        self._error_count += 1
        self._last_error = message
        self.emit('errors', message)

    def _filename(self, item: MediaItem) -> str:
        """Upload name: the source file's stem, but always the extension of
        the payload's real format - a segment cut from ``talk.m4a`` is WAV
        data, and servers pick their decoder by extension."""
        stem = os.path.splitext(os.path.basename(str(item.meta.get('filename') or '')))[0] or 'audio'
        return f"{stem}.{extension_for_mime(item.mime)}"

    async def _transcribe_api(self, item: MediaItem, client: httpx.AsyncClient) -> dict:
        data = await item.aget_bytes()
        form = {'model': self.model, 'response_format': 'verbose_json'}
        if self.language:
            form['language'] = self.language
        if self.prompt:
            form['prompt'] = self.prompt
        headers = {'Authorization': f'Bearer {self.api_key}'} if self.api_key else {}
        resp = await client.post(
            f"{self.base_url}/audio/transcriptions",
            data=form, files={'file': (self._filename(item), data, item.mime)}, headers=headers,
        )
        if resp.status_code >= 400:
            raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
        try:
            body = resp.json()
        except ValueError:
            return {'text': resp.text}
        segments = [
            {'start': s.get('start'), 'end': s.get('end'), 'text': (s.get('text') or '').strip()}
            for s in body.get('segments') or [] if isinstance(s, dict)
        ]
        return {'text': body.get('text', ''), 'language': body.get('language'),
                'duration': body.get('duration'), 'segments': segments}

    def _transcribe_local(self, item: MediaItem) -> dict:
        model = _load_local_model(self.model, self.device, self.compute_type)
        segments, info = model.transcribe(
            io.BytesIO(item.get_bytes()), language=self.language, initial_prompt=self.prompt,
            beam_size=self.beam_size,
        )
        segs = [{'start': round(s.start, 3), 'end': round(s.end, 3), 'text': s.text.strip()} for s in segments]
        return {'text': ' '.join(s['text'] for s in segs), 'language': getattr(info, 'language', None),
                'duration': getattr(info, 'duration', None), 'segments': segs}
