import asyncio
import logging
import queue

from ..core.node import BaseNode

_logger = logging.getLogger('pystreamflow.nodes.input_log')


class _QueueLogHandler(logging.Handler):
    """A ``logging.Handler`` that pushes each record it receives onto a
    plain (thread-safe) ``queue.Queue`` instead of formatting it to a
    stream. ``logging`` calls can happen on any thread, but
    ``LogInputNode.process()`` runs on the asyncio event loop - routing
    through a thread-safe queue lets the two sides meet without either
    one needing to know about the other's concurrency model.
    """

    def __init__(self, q: "queue.Queue"):
        super().__init__()
        self._q = q

    def emit(self, record: logging.LogRecord):
        try:
            self._q.put_nowait({
                'level': record.levelname,
                'logger': record.name,
                'msg': record.getMessage(),
                'timestamp': record.created,
            })
        except Exception:
            # Standard logging.Handler contract: never let emit() raise
            # out into the caller that logged the record.
            self.handleError(record)


class LogInputNode(BaseNode):
    """Log Input Node.

    Attaches a real ``logging.Handler`` to a Python ``logging`` logger
    and emits each log record that logger actually receives - not a
    canned sample. Pairs naturally with ``LogOutputNode``
    (``pystreamflow/nodes/output_log.py``), which really calls into
    ``logging`` for every item it processes: wiring a ``LogOutputNode``
    upstream of a differently-scoped ``LogInputNode`` (or attaching to
    the root logger to catch everything) turns "things this process
    logged" into real graph data.

    Config:
      logger_name: name of the logger to attach to; ``''`` (the default)
        attaches to the root logger, so records from every logger in
        this process that propagate to root are captured.
      level: minimum level to capture, e.g. ``'INFO'``, ``'DEBUG'``,
        ``'WARNING'`` (default ``'INFO'``) - matches the names in
        Python's ``logging`` module. Live-updatable via an attribute
        wire (``set_attribute``); a new value takes effect on this
        node's next poll of its internal queue (at most ~1s later).

    Note on logger levels: Python's ``logging`` module filters a record
    against the *logger's* effective level before it ever reaches a
    handler - a handler's own level can only filter further, never
    loosen that. A fresh (or root) logger defaults to ``WARNING``, so
    attaching this node with ``level: 'INFO'`` to a logger nobody has
    configured would otherwise silently see nothing below WARNING no
    matter what this node's handler is set to. To make ``level`` actually
    mean what it says, ``init()`` lowers the target logger's own level to
    match whenever it's currently stricter (never raises it - a logger
    someone already set to DEBUG stays at DEBUG), and ``stop()`` restores
    whatever level the logger had before this node touched it.
    """

    async def init(self):
        self.logger_name = self.config.get('logger_name', '')
        self.level = self.config.get('level', 'INFO')
        self._queue: queue.Queue = queue.Queue()
        self._handler = _QueueLogHandler(self._queue)
        self._target_logger = logging.getLogger(self.logger_name or None)
        self._original_logger_level = self._target_logger.level
        self._apply_level()
        self._target_logger.addHandler(self._handler)

    def _apply_level(self):
        level_num = getattr(logging, str(self.level).upper(), logging.INFO)
        self._handler.setLevel(level_num)
        # See the "Note on logger levels" above: NOTSET (0) means "defer
        # to the effective/inherited level", which for the root logger is
        # WARNING - stricter than our own default of INFO - so treat
        # NOTSET as "at least as strict as anything" for this comparison.
        current = self._target_logger.level or logging.WARNING
        if level_num < current:
            self._target_logger.setLevel(level_num)

    async def process(self):
        while self._running:
            # Re-applied every pass so a live update to `level` via
            # set_attribute() takes effect without needing a restart.
            self._apply_level()
            try:
                record = await asyncio.to_thread(self._queue.get, True, 1.0)
            except queue.Empty:
                continue
            self.emit('out', record)

    async def stop(self):
        try:
            self._target_logger.removeHandler(self._handler)
            self._target_logger.setLevel(self._original_logger_level)
        except Exception:
            # Cleanup best-effort: a failure here shouldn't block the
            # node from stopping, but it's worth a trace for whoever's
            # debugging why a logger's level didn't get restored.
            _logger.debug('node %s: failed to detach log handler cleanly', self.id, exc_info=True)
        await super().stop()
