from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_input, register_node, start_server
import asyncio

class WebInputNode(BaseNode):
    async def init(self):
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.pipe = self.add_output_if_unwired('out', Pipe())
        # Bug fix: this used to default to '127.0.0.1', unreachable
        # through the Docker-published port for this node's shared web
        # server - see ApiInputNode.init()'s comment for the full
        # explanation (Docker's port-forwarding never reaches a
        # container-internal loopback bind) and how this was confirmed.
        self.host = self.config.get('host', '0.0.0.0')
        self.port = self.config.get('port', 8080)
        self.path = self.config.get('path', f'/in/{self.id}')
        self.node_id = self.id or f"web_{id(self)}"
        # Register node for live view
        register_node(self)
        # Register route with shared server
        register_input(self.node_id, self.path, self)

    async def process(self):
        # Start shared server once if not running
        await start_server(self.host, self.port)
        # Keep node alive, pipe is handled by server
        while self._running:
            await asyncio.sleep(1)

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `path` is the
        one config field this node type's already-registered HTTP route
        is derived from - re-registering (register_input() ->
        register_route() safely replaces the old route under this node's
        stable route name) is what makes editing it on a live node
        actually take effect.
        """
        if 'path' not in changed_keys:
            return
        self.path = self.config.get('path', f'/in/{self.id}')
        register_input(self.node_id, self.path, self)

    async def stop(self):
        await super().stop()
