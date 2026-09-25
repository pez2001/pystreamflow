from ..core.node import BaseNode
import asyncio
import httpx

class UrlInputNode(BaseNode):
    async def init(self):
        self.url = self.config.get('url')
        if not self.url:
            raise ValueError('UrlInputNode requires url config')
        self.method = self.config.get('method', 'GET').upper()
        self.headers = self.config.get('headers', {})
        self.poll_interval = float(self.config.get('poll_interval', 5.0))
        self.timeout = float(self.config.get('timeout', 10.0))

    async def process(self):
        while self._running:
            try:
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    if self.method == 'GET':
                        resp = await client.get(self.url, headers=self.headers)
                    elif self.method == 'POST':
                        resp = await client.post(self.url, headers=self.headers, json=self.config.get('json_body'))
                    else:
                        resp = await client.request(self.method, self.url, headers=self.headers)
                    resp.raise_for_status()
                    # emit text or json
                    try:
                        data = resp.json()
                    except Exception:
                        data = resp.text
                    self.emit('out', data)
            except Exception as e:
                self._last_error = str(e)
            await asyncio.sleep(self.poll_interval)
