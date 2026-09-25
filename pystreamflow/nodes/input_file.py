from ..core.node import BaseNode
from ..core.stream import Pipe
import asyncio
from pathlib import Path

class FileInputNode(BaseNode):
    async def init(self):
        self.path = self.config.get('path')
        if not self.path:
            raise ValueError('FileInputNode requires path config')
        self.poll_interval = float(self.config.get('poll_interval', 1.0))
        # Phase 1 fan-out audit found this the one node type reading
        # self.outputs.get('out') directly instead of going through
        # add_output_if_unwired() - every sibling "self-contained" node
        # type (see that method's own docstring for the full list) uses
        # the latter specifically so it still has a usable output pipe
        # when driven directly (e.g. in a test) without an Engine having
        # wired anything real yet. This was harmless in practice only
        # because nothing on this class ever read self.out_pipe again -
        # but self.outputs['out'] is now a list of consumer pipes rather
        # than a single Pipe (see core/node.py), which would have made
        # self.out_pipe silently the wrong shape the moment anything did
        # start relying on it. Fixed the same way as its siblings.
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
    
    async def process(self):
        # `path` (and `last_size`) used to be computed once here, before
        # this loop started, which meant a live attribute-wire update to
        # `self.path` (e.g. from a ListStringsNode feeding in a new
        # filename) updated self.config['path']/self.path correctly but
        # had zero effect on this already-running node - it kept tailing
        # whatever file it started with until manually restarted. Fixed
        # by re-reading self.path on every iteration and detecting a
        # change, so live attribute-wiring actually works: switching to a
        # new path starts tailing that file from its beginning.
        last_size = 0
        last_path_str = None
        path = None
        while self._running:
            if self.path != last_path_str:
                last_path_str = self.path
                path = Path(self.path) if self.path else None
                last_size = 0
            if path is None or not path.exists():
                await asyncio.sleep(self.poll_interval)
                continue
            try:
                size = path.stat().st_size
                if size > last_size:
                    data = path.read_bytes()[last_size:]
                    last_size = size
                    if data:
                        self.emit('out', data)
                else:
                    await asyncio.sleep(self.poll_interval)
            except Exception as e:
                self._last_error = str(e)
                await asyncio.sleep(self.poll_interval)
