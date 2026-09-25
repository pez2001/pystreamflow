from ..core.node import BaseNode
from ..core.stream import Pipe
from ..core.web_server import register_input, register_node, start_server
import asyncio


class ApiInputNode(BaseNode):
    """A REST-style input node: exposes a single POST endpoint under the
    shared web server's ``/api`` prefix and emits each request's JSON body
    onto its ``out`` port.

    This is directly modeled on ``WebInputNode`` (same shared
    ``core/web_server.py`` app, same ``register_input()``/``register_node()``/
    ``start_server()`` plumbing, same "call start_server() from process(),
    not init()" pattern so a not-yet-running event loop isn't required at
    construction time) - the only real difference is the one user-facing
    config attribute, ``uri``, which is normalized into a path under
    ``/api/`` rather than accepting an arbitrary path directly the way
    ``WebInputNode``'s ``path`` config does. For example a ``uri`` of
    ``"orders"`` (or ``"/orders"``, or ``"orders/"`` - all equivalent)
    maps to the route ``POST /api/orders``.

    ``uri`` defaults to the node's own id when unset *or* left blank -
    the node editor's palette shows ``uri`` as an editable text field
    (see ``editor.js``'s ``DEFAULT_CONFIG``) seeded with ``''`` so it's
    visible and editable from the moment the node is dropped on the
    canvas, which means a fresh node's config always has the key present
    (unlike a key that's truly absent). Falling back on an empty string
    too - not just a missing key - is what makes that still resolve to
    the node's own id instead of a broken ``/api/`` route with no
    trailing segment.
    """

    async def init(self):
        self.pipe = self.add_output_if_unwired('out', Pipe())
        # Bug fix (found while reproducing "sent a real HTTP request to
        # /api/<uri>, nothing arrived" for a workflow running under
        # Docker): this used to default to '127.0.0.1', which - unlike on
        # a bare-metal/venv install - means something completely
        # different inside a container. `docker-compose.yml` publishes
        # this node's shared web server port (default 8080) to the host
        # with `"8080:8080"`, but Docker's port-forwarding connects an
        # incoming host-side connection to the container's real network
        # interface, never to the container's own loopback. A server
        # bound to 127.0.0.1 *inside* the container is therefore
        # unreachable through that published port from anywhere outside
        # that one container - not from the Docker host, not from the
        # LAN, not from another container - even though `docker ps`/the
        # compose file both look completely correct and nothing logs an
        # error anywhere. Confirmed via a real run: with the old default,
        # `/proc/net/tcp` showed the listening socket bound to literal
        # 127.0.0.1, not 0.0.0.0 (all interfaces). Every node type that
        # shares this same `core/web_server.py` app (`WebInputNode`,
        # `ApiOutputNode`, `WebOutputNode`, `WebOutputJSONNode`) had the
        # identical bug, fixed identically; `SocketInputNode` already
        # defaulted to `0.0.0.0` and was never affected. Set `host` to
        # `127.0.0.1` explicitly in this node's config if you specifically
        # want same-container-only reachability instead.
        self.host = self.config.get('host', '0.0.0.0')
        self.port = self.config.get('port', 8080)
        self.node_id = self.id or f"api_{id(self)}"
        self.uri = str(self.config.get('uri') or self.node_id).strip('/')
        self.path = f"/api/{self.uri}"
        register_node(self)
        register_input(self.node_id, self.path, self)

    async def process(self):
        await start_server(self.host, self.port)
        while self._running:
            await asyncio.sleep(1)

    def on_config_updated(self, changed_keys):
        """See BaseNode.on_config_updated()'s docstring. `uri` is the one
        config field this node type derives its live HTTP route from -
        set_attribute() (called just before this) already mirrored the
        *raw* new value onto self.uri, but not through the same
        fallback-to-node-id/strip('/') normalization init() applies, and
        it never touches self.path or the already-registered route at
        all. Re-deriving both here and re-registering (register_input()
        -> register_route() safely replaces the old route under this
        node's stable route name) is what actually makes editing `uri`
        on a live node take effect.
        """
        if 'uri' not in changed_keys:
            return
        self.uri = str(self.config.get('uri') or self.node_id).strip('/')
        self.path = f"/api/{self.uri}"
        register_input(self.node_id, self.path, self)

    async def stop(self):
        await super().stop()
