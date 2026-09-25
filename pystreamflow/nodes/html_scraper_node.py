from ..core.node import BaseNode
import asyncio
import re

class HTMLScraperNode(BaseNode):
    async def init(self):
        self.extractors = self.config.get('extractors', [])
        self.default_output = self.config.get('output_key', 'scraped')
        try:
            from bs4 import BeautifulSoup
            self.bs4 = BeautifulSoup
        except Exception:
            self.bs4 = None

    async def process(self):
        while self._running:
            pipe_in = next(iter(self.inputs.values()), None)
            if not pipe_in:
                await asyncio.sleep(0.1)
                continue
            try:
                item = await asyncio.wait_for(pipe_in.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            html = str(item)
            result = {}

            if self.bs4:
                soup = self.bs4(html, 'html.parser')
                for ex in self.extractors:
                    name = ex.get('name') or 'field'
                    css = ex.get('css_selector')
                    attr = ex.get('attr')
                    text_only = ex.get('text_only', True)
                    if css:
                        els = soup.select(css)
                        if attr:
                            vals = [e.get(attr, '') for e in els]
                        else:
                            vals = [e.get_text(strip=True) if text_only else str(e) for e in els]
                        result[name] = vals[0] if len(vals) == 1 else vals
                    else:
                        # fallback regex
                        pattern = ex.get('pattern')
                        if pattern:
                            flags = re.IGNORECASE if ex.get('ignore_case') else 0
                            result[name] = re.findall(pattern, html, flags)
            else:
                # pure regex fallback
                for ex in self.extractors:
                    name = ex.get('name') or 'field'
                    pattern = ex.get('pattern')
                    if not pattern:
                        continue
                    flags = re.IGNORECASE if ex.get('ignore_case') else 0
                    matches = re.findall(pattern, html, flags)
                    result[name] = matches[0] if len(matches) == 1 else matches

            if not self.extractors:
                # default: return stripped text
                result[self.default_output] = re.sub(r'<[^>]+>', '', html).strip()

            self.emit('out', result)
