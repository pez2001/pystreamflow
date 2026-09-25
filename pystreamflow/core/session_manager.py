"""
Session management for PyStreamFlow workflows.
Supports headless execution with per-session isolation.
"""
import time
import uuid
import asyncio
import logging
from typing import Dict, Optional
from ..core.persistence import load_workflow
from ..core.engine import Engine
from ..core.models import Graph

logger = logging.getLogger("pystreamflow.session")

class Session:
    def __init__(self, session_id: str, workflow_path: str):
        self.id = session_id
        self.workflow_path = workflow_path
        self.graph: Optional[Graph] = None
        self.engine: Optional[Engine] = None
        self.status = "created"  # created, running, paused, stopped, error
        self.task: Optional[asyncio.Task] = None
        # time.monotonic() works whether or not an event loop is running,
        # unlike asyncio.get_event_loop().time() which raises RuntimeError
        # when Session() is constructed from sync code (e.g. SessionManager
        # .create() called outside a coroutine) on Python 3.10+.
        self.created_at = time.monotonic()
        self._stop_event = asyncio.Event()

    async def load(self):
        self.graph = load_workflow(self.workflow_path)
        # session_id=self.id lets every node this Engine instantiates be
        # attributed to this Session in the global node registry (see
        # web_server.register_node()'s docstring) - without it, running
        # the same workflow file in two Sessions concurrently (or one
        # Session plus the ad-hoc node editor) silently collided.
        self.engine = Engine(self.graph, session_id=self.id)
        # Validate
        self.engine.validate()

    async def start(self):
        if self.status == "running":
            return
        if not self.engine:
            await self.load()
        self.status = "running"
        self._stop_event.clear()
        # Run engine in background task
        self.task = asyncio.create_task(self._run_loop())

    async def _run_loop(self):
        # The old version of this had a `finally: self.status = "stopped"`
        # that ran after every branch, including the `except Exception`
        # one - so it unconditionally clobbered "error" back to "stopped"
        # before anyone (API/CLI/MCP) could ever observe a session that
        # had actually crashed, and the exception itself was dropped with
        # just a "# Log error" comment and no logging call. That is exactly
        # the kind of blind-except-that-hides-failures this Phase 1 pass
        # is otherwise fixing at the node/engine level via supervision -
        # Session had the same problem one layer up.
        try:
            # Engine.run is long-running; we wrap it
            await self.engine.run()
            self.status = "stopped"
        except asyncio.CancelledError:
            self.status = "stopped"
            raise
        except Exception:
            self.status = "error"
            logger.exception("session %s: engine.run() raised an unhandled error", self.id)

    async def stop(self):
        self.status = "stopped"
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        self._stop_event.set()

    async def pause(self):
        # Previously this just called self.stop(), which (a) immediately
        # overwrote self.status back to "stopped" (stop() sets it
        # unconditionally), so a "paused" session actually reported itself
        # as stopped, and (b) cancelled self.task, tearing down the
        # engine's run()/_supervise() loop and every node entirely - there
        # was no way back short of calling start() again, which re-runs
        # Engine.run() from scratch (re-instantiating every node via
        # _instantiate_nodes(), losing all state) rather than resuming.
        # There was also no Session.resume() at all.
        #
        # Now pause() leaves self.task (and the engine object, its
        # node_instances, and its pipes) running and intact, and just
        # suspends each node's process() loop via Engine.pause_all() ->
        # BaseNode.pause() - the same real suspend/resume this session's
        # Phase 1 work added at the node level.
        if self.status != "running":
            return
        self.status = "paused"
        if self.engine:
            await self.engine.pause_all()

    async def resume(self):
        if self.status != "paused":
            return
        self.status = "running"
        if self.engine:
            await self.engine.resume_all()

    def info(self):
        return {
            "id": self.id,
            "workflow_path": self.workflow_path,
            "status": self.status,
            "nodes": len(self.graph.nodes) if self.graph else 0,
            "edges": len(self.graph.edges) if self.graph else 0,
        }

class SessionManager:
    def __init__(self):
        self.sessions: Dict[str, Session] = {}

    def create(self, workflow_path: str) -> Session:
        sid = str(uuid.uuid4())
        sess = Session(sid, workflow_path)
        self.sessions[sid] = sess
        return sess

    def get(self, session_id: str) -> Optional[Session]:
        return self.sessions.get(session_id)

    def list_sessions(self):
        return [s.info() for s in self.sessions.values()]

    def delete(self, session_id: str) -> bool:
        sess = self.sessions.get(session_id)
        if not sess:
            return False
        # Stop if running
        # Note: can't await here in sync context
        if sess.task and not sess.task.done():
            # schedule stop
            asyncio.create_task(sess.stop())
        del self.sessions[session_id]
        # Bug fix (multi-session verification pass): nothing ever removed
        # a deleted session's nodes from the global node registry
        # (core/web_server.py's `_nodes`/`_session_nodes`) - they used to
        # linger there forever, still answering health/reflection queries
        # for a session that no longer exists, and permanently occupying
        # their node ids' registry slots even after the workflow that
        # created them was gone. This is plain dict bookkeeping (no
        # asyncio needed), so it's safe to do synchronously here rather
        # than waiting on the fire-and-forget stop() task above.
        from .web_server import unregister_session
        unregister_session(session_id)
        return True

# Global singleton
session_manager = SessionManager()
