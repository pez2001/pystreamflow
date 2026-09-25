from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_node
import asyncio
import httpx
import time

class LMStudioNode(BaseNode):
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
        register_node(self)

    async def process(self):
        client_timeout = httpx.Timeout(10.0, read=self.timeout)
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
                if isinstance(prompt, str):
                    messages.append({"role": "user", "content": prompt})
                else:
                    messages.append({"role": "user", "content": str(prompt)})

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
