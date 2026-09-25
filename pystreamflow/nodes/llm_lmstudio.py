from ..core.media import MediaItem
from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node
import asyncio
import base64
import httpx
import time

class LMStudioNode(BaseNode):
    """Chat completion against an OpenAI-compatible server (LM Studio,
    llama.cpp, vLLM, ...).

    Vision (media plan phase 3): an image ``MediaItem`` arriving on ``in``
    - alone, as a list, or as a dict like ``{"image": item, "prompt":
    "..."}`` - is sent as an ``image_url`` content part with a
    ``data:<mime>;base64,...`` URL, the format vision models on these
    servers accept. The accompanying text is, in order: the dict's
    ``prompt``/``text`` value, the latest value that arrived on the
    ``prompt`` input port, or the ``image_prompt`` config (default
    "Describe this image."). The model has to support images; a text-only
    model will answer with an error from the server. Shrink large images
    first (ImageResizeNode) - every pixel costs tokens.
    """

    async def init(self):
        # Bug fix found while auditing the ad-hoc "POST /nodes then POST
        # /nodes/connect" node-creation path (api/server.py's create_node
        # now auto-starts a node the moment it's created, instead of
        # leaving it inert until a separate manual start - see that
        # change's own comment): this used to leave self.in_pipe unset in
        # init() and only resolve `self.inputs.get('in')` once, at the top
        # of process(), before the while loop. Since process() starts
        # running immediately on start(), a node created and auto-started
        # first and wired up via /nodes/connect a moment later would have
        # already captured a stale (or freshly self-created, disconnected)
        # pipe reference by the time the real wire arrived - the real
        # upstream data would then never reach it. Using
        # add_input_if_unwired() here (the same fix already applied to
        # every other node type with this "self-wire if nothing external
        # showed up first" fallback - see its own docstring) and
        # re-resolving `self.inputs.get('in', self.in_pipe)` on every loop
        # iteration in process() (below) instead of once outside it means
        # a late-arriving real wire is picked up within one poll interval
        # instead of never.
        self.in_pipe = self.add_input_if_unwired('in', Pipe())
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self.base_url = self.config.get('base_url', 'http://localhost:1234/v1')
        self.model = self.config.get('model', 'local-model')
        self.api_key = self.config.get('api_key', 'lm-studio')
        self.system = self.config.get('system', '')
        # Bug fix (real-world report): a flat 60s httpx timeout used to
        # cover the whole request - connect, write, AND read - for the
        # life of this node's one shared client. That's nowhere near
        # enough for a slow local model: the reporter's own llama.cpp
        # server log showed a single completion taking 218s for 795
        # output tokens (~3.6 tokens/sec, not unusual for a large model on
        # modest hardware), and the server-side symptom of httpx aborting
        # the still-in-progress read at 60s is exactly
        # "http client error: Connection handling canceled" - the client
        # dropping the connection mid-generation, not a real server error.
        # `timeout` is now configurable (default 600s/10min, generous
        # enough for most local setups) and split so only the READ leg
        # (waiting for tokens) gets that long budget - connect/write stay
        # at a short 10s, so a genuinely unreachable/down server still
        # fails fast instead of hanging for the full timeout too.
        self.timeout = float(self.config.get('timeout', 600.0))
        self.image_prompt = self.config.get('image_prompt') or 'Describe this image.'
        self._latest_prompt = None
        register_node(self)

    async def _prompt_listener(self):
        """Keeps the latest text from the optional ``prompt`` input port,
        used as the question for the next image."""
        while self._running:
            pipe = self.inputs.get('prompt')
            if pipe is None:
                await asyncio.sleep(0.5)
                continue
            try:
                value = await asyncio.wait_for(pipe.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if isinstance(value, dict) and 'value' in value:
                value = value['value']
            self._latest_prompt = None if value is None else str(value)

    @staticmethod
    def _images_in(prompt) -> list:
        if isinstance(prompt, MediaItem):
            candidates = [prompt]
        elif isinstance(prompt, (list, tuple)):
            candidates = list(prompt)
        elif isinstance(prompt, dict):
            candidates = []
            for value in prompt.values():
                candidates.extend(value if isinstance(value, (list, tuple)) else [value])
        else:
            return []
        return [c for c in candidates if isinstance(c, MediaItem) and c.kind in ('image', 'video_frame')]

    async def _user_content(self, prompt):
        """The user message's ``content``: a plain string for text, or a
        list of text + image_url parts when the item carries images."""
        images = self._images_in(prompt)
        if not images:
            return prompt if isinstance(prompt, str) else str(prompt)
        text = None
        if isinstance(prompt, dict):
            text = prompt.get('prompt') or prompt.get('text')
        text = text or self._latest_prompt or self.image_prompt
        parts = [{"type": "text", "text": str(text)}]
        for image in images:
            data = await image.aget_bytes()
            url = f"data:{image.mime};base64,{base64.b64encode(data).decode('ascii')}"
            parts.append({"type": "image_url", "image_url": {"url": url}})
        return parts

    async def process(self):
        client_timeout = httpx.Timeout(10.0, read=self.timeout)
        prompt_task = asyncio.create_task(self._prompt_listener())
        try:
            await self._serve(client_timeout)
        finally:
            prompt_task.cancel()

    async def _serve(self, client_timeout):
        async with httpx.AsyncClient(timeout=client_timeout) as client:
            while self._running:
                # Re-resolved every iteration rather than captured once -
                # see the note in init() above for why.
                input_pipe = self.inputs.get('in', self.in_pipe)
                try:
                    prompt = await asyncio.wait_for(input_pipe.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                messages = []
                if self.system:
                    messages.append({"role": "system", "content": self.system})
                try:
                    content = await self._user_content(prompt)
                except KeyError:
                    # the image's blob expired before it could be sent
                    self.emit('errors', 'image payload expired before it could be sent')
                    self.emit('out', {"prompt": prompt, "error": "image payload expired"})
                    continue
                messages.append({"role": "user", "content": content})

                # Feature request: separate named outputs instead of one
                # 'out' port carrying either a {'prompt','completion'} or a
                # {'prompt','error'} dict - a downstream graph used to have
                # to branch on which shape it got. 'prompt' fires
                # unconditionally (useful for logging/correlating what was
                # sent even before a response comes back); 'out' keeps
                # carrying the full picture for anything that wants it all
                # in one item, unchanged in spirit from before.
                self.emit('prompt', prompt)
                start = time.monotonic()
                try:
                    resp = await client.post(
                        f"{self.base_url}/chat/completions",
                        headers={"Authorization": f"Bearer {self.api_key}"},
                        json={
                            "model": self.model,
                            "messages": messages,
                            "stream": False
                        }
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    elapsed = time.monotonic() - start
                    message = data.get("choices", [{}])[0].get("message", {}) or {}
                    content = message.get("content") or ""
                    # LM Studio serves some "thinking"/reasoning models
                    # (e.g. DeepSeek-R1 variants) that report their chain
                    # of thought separately as message.reasoning_content
                    # (the field name recent LM Studio/OpenAI-compatible
                    # servers use) or message.reasoning; older servers that
                    # don't split it out at all instead inline it as a
                    # <think>...</think> block at the start of `content` -
                    # handled below so 'reasoning'/'results' still separate
                    # cleanly even from a server that predates the field.
                    reasoning = message.get("reasoning_content") or message.get("reasoning") or ""
                    results = content
                    if not reasoning and "<think>" in content and "</think>" in content:
                        think_start = content.find("<think>")
                        think_end = content.find("</think>")
                        if think_start != -1 and think_end != -1 and think_end > think_start:
                            reasoning = content[think_start + len("<think>"):think_end].strip()
                            results = (content[:think_start] + content[think_end + len("</think>"):]).strip()
                    usage = data.get("usage") or {}
                    stats = {
                        "prompt_tokens": usage.get("prompt_tokens"),
                        "completion_tokens": usage.get("completion_tokens"),
                        "total_tokens": usage.get("total_tokens"),
                        "latency_s": round(elapsed, 3),
                        "model": data.get("model", self.model),
                    }
                    self.emit('results', results)
                    if reasoning:
                        self.emit('reasoning', reasoning)
                    self.emit('stats', stats)
                    self.emit('out', {
                        "prompt": prompt,
                        "completion": content,
                        "results": results,
                        "reasoning": reasoning or None,
                        "usage": usage,
                    })
                except Exception as e:
                    self.emit('errors', str(e))
                    self.emit('out', {"prompt": prompt, "error": str(e)})
