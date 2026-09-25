import asyncio
import os

from ..core.node import BaseNode
from ..core.paths import ensure_parent_dir, files_dir


class FileOutputNode(BaseNode):
    """File Output Node.

    Appends each incoming item, one per line, to a real file on disk.

    Bug found and fixed while wiring this node type into the project's
    new logs/files/data Docker volume mounts: despite the class name,
    its "Append items to file" description in the node editor, and a
    `path` config field that looks exactly like every other real-I/O
    node's, this had *never* actually touched the filesystem at all -
    the body of its loop was a bare comment (`# Simulate write`)
    followed only by a call into the live-view bookkeeping helper
    (`record_output()`). A user who wired this node into a workflow got
    an item count going up and a plausible-looking live view, and no
    file, ever, anywhere - a silent no-op with every outward sign of
    working. Now it really writes.

    Config:
      path: file to append to (default: '<files_dir>/output.txt', where
        files_dir is `PSF_FILES_DIR` - '/app/files' under the packaged
        docker-compose.yml, so this lands on a real, host-visible mount
        by default instead of a container-only path nobody can see, the
        way the old hardcoded '/tmp/out.txt' default did)
      append_newline: append '\\n' to each item's encoded text if it
        doesn't already end with one (default True)
    """

    async def init(self):
        self.path = self.config.get('path') or os.path.join(files_dir(), 'output.txt')
        self.append_newline = bool(self.config.get('append_newline', True))

    async def process(self):
        # Bug fix (found live, from a direct report against a different
        # node type with the identical shape - modifier_json.py/
        # JSONExtractNode): `pipe` used to be captured once, before this
        # loop even started, so a wire arriving via POST /nodes/connect
        # after this node was created and auto-started (the normal ad-hoc
        # node-editor sequence) was never seen even though this loop kept
        # running. Re-fetching `pipe` every outer iteration picks up a late
        # wire within one pass instead of never.
        while self._running:
            pipe = self.inputs.get('in')
            if pipe:
                try:
                    item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                    self._write(item)
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)

    def _write(self, item):
        payload = item if isinstance(item, (bytes, bytearray)) else str(item).encode('utf-8')
        if self.append_newline and not payload.endswith(b'\n'):
            payload = payload + b'\n'
        try:
            ensure_parent_dir(self.path)
            with open(self.path, 'ab') as f:
                f.write(payload)
        except OSError as e:
            self._last_error = str(e)
            # Bug fix (found while fixing the identical, directly-
            # reported issue on MQTTOutputNode - see BaseNode.
            # record_output()): appending straight to self._last_items
            # here, bypassing both emit() and record_output(), skipped
            # the _max_last cap entirely - a long-running FileOutputNode's
            # live-view buffer grew without bound instead of staying
            # capped at config['max_last'] the way every other node
            # type's does.
            self.record_output({'written': item, 'path': self.path, 'error': str(e)})
            return
        self.record_output({'written': item, 'path': self.path, 'bytes_written': len(payload)})
