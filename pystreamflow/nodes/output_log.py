import asyncio
import logging
import os

from ..core.node import BaseNode
from ..core.paths import ensure_parent_dir, logs_dir

_logger = logging.getLogger('pystreamflow.nodes.output_log')

# Paths a real logging.FileHandler has already been attached to, for this
# process's lifetime. Multiple LogOutputNode instances (or the same node
# id created again after a restart) all log through the one shared
# `_logger` above - without this guard, each new instance would stack
# another FileHandler onto it and every future line would be written
# once per still-attached handler (duplicated 2x, 3x, ... instead of
# once), rather than sharing the one real file the way the log lines
# visually promise.
_attached_file_handlers: set = set()


def _attach_file_handler(path: str) -> None:
    if path in _attached_file_handlers:
        return
    ensure_parent_dir(path)
    handler = logging.FileHandler(path, encoding='utf-8')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s'))
    _logger.addHandler(handler)
    # The per-record level passed to _logger.log() below (INFO/DEBUG/...)
    # is what actually gates each line - this only has to make sure the
    # logger itself doesn't filter everything out at its own level before
    # the handler ever sees it.
    if _logger.level == logging.NOTSET or _logger.level > logging.DEBUG:
        _logger.setLevel(logging.DEBUG)
    _attached_file_handlers.add(path)


class LogOutputNode(BaseNode):
    """Log Output Node.

    Logs each incoming item at ``config['level']`` (default ``INFO``)
    through Python's standard ``logging`` module - and, new in this
    update, *also* into a real log file on disk (``config['file']``,
    default ``'<logs_dir>/pystreamflow.log'`` where ``logs_dir`` is
    ``PSF_LOGS_DIR`` - ``/app/logs`` under the packaged
    ``docker-compose.yml``). Previously this only ever reached whatever
    handler the process's root logger happened to have (uvicorn's own
    console handler under the daemon, nothing at all under a bare
    script) - so "log output" never actually produced a log *file*
    anywhere an operator could open it, despite the node's name and the
    project's own ``logs`` Docker volume mount sitting right there
    unused. The item this node emits downstream is unchanged by this -
    ``{'level', 'log'}`` exactly as before - only the real side effect
    (what gets written to disk) is new.

    Config:
      level: log level name, e.g. ``'INFO'``/``'WARNING'``/``'ERROR'``
        (default ``'INFO'``, falls back to INFO for an unrecognized name)
      file: path to the real log file to append to (default:
        ``'<logs_dir>/pystreamflow.log'``)
    """

    async def init(self):
        self.level = self.config.get('level', 'INFO')
        self.file = self.config.get('file') or os.path.join(logs_dir(), 'pystreamflow.log')
        try:
            _attach_file_handler(self.file)
        except OSError as e:
            self._last_error = f'could not open log file {self.file!r}: {e}'

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
                    # Despite the class name and its `level` config field,
                    # this used to never call into Python's logging module
                    # at all - it only tagged the item with the configured
                    # level and re-emitted it downstream, so nothing was
                    # ever actually logged anywhere. Now it really does
                    # log, at the configured level (falling back to INFO
                    # for an unrecognized level name), in addition to the
                    # existing tag-and-forward behavior other nodes/tests
                    # rely on (the emitted shape is unchanged).
                    level_num = getattr(logging, str(self.level).upper(), logging.INFO)
                    _logger.log(level_num, '[%s] %r', self.id, item)
                    self.emit('out', {'level': self.level, 'log': item})
                except asyncio.TimeoutError:
                    continue
            else:
                await asyncio.sleep(0.5)
