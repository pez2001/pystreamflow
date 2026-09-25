from ..core.node import BaseNode
from ..core.stream import Pipe
import asyncio
import json
import logging
import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

class MQTTInputNode(BaseNode):
    async def init(self):
        self.broker = self.config.get('broker', 'localhost')
        self.port = int(self.config.get('port', 1883))
        self.topic = self.config.get('topic', '#')
        self.client_id = self.config.get('client_id', f'pystreamflow_{self.id}')
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.qos = int(self.config.get('qos', 0))
        # Phase 6 hardening fix: see BaseNode.add_output_if_unwired()'s
        # docstring - a plain add_output() here used to clobber the real
        # downstream pipe the Engine had already wired before start().
        self.out_pipe = self.add_output_if_unwired('out', Pipe())
        self._client = None
        self._connected = asyncio.Event()
        # Set for real in process(), before paho's network thread starts -
        # see _on_message()'s docstring for why this is needed at all.
        self._loop = None

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected.set()
            client.subscribe(self.topic, qos=self.qos)

    def _on_message(self, client, userdata, msg):
        """paho invokes this from its own background network thread (see
        process(): `client.loop_forever()` runs inside a plain
        threading.Thread, not on this node's asyncio event loop) - except
        in this project's own tests, which call it directly from the
        event-loop thread to exercise the callback logic without a real
        broker (see tests/test_mqtt_coverage.py).

        Bug fixed here (Phase 6 hardening audit): this used to call
        `asyncio.create_task(self.out_pipe.put(item))` unconditionally.
        `asyncio.create_task()` requires a running event loop *in the
        calling thread* - it has none in paho's real network thread - so
        every real (non-test) MQTT message silently failed to schedule its
        own delivery with `RuntimeError: no running event loop`, and that
        RuntimeError was itself swallowed by the blind `except Exception:
        pass` this method used to end with. Net effect: MQTTInputNode
        looked like it worked (connected, subscribed, no visible errors)
        but never actually delivered a single real message anywhere.
        Fixed by detecting which situation we're in and scheduling the
        delivery onto the *right* thread's event loop either way:
        `asyncio.get_running_loop()` succeeds when called from the same
        thread as a running loop (the test scenario - schedule directly
        with create_task), and raises RuntimeError otherwise (the real
        threaded scenario - hand the coroutine to the loop captured in
        process() via the thread-safe `run_coroutine_threadsafe`).
        Delivering through self.emit() rather than self.out_pipe.put()
        directly is also what makes the add_output_if_unwired() fix above
        actually take effect for this node - emit() looks up
        self.outputs['out'] at call time, while writing to self.out_pipe
        directly would keep bypassing whatever's really wired there.
        """
        try:
            payload = msg.payload.decode('utf-8', errors='replace')
            try:
                data = json.loads(payload)
            except Exception:
                data = payload
            item = {
                'topic': msg.topic,
                'payload': data,
                'qos': msg.qos,
                'retain': msg.retain
            }
            try:
                asyncio.get_running_loop()
                asyncio.create_task(self._deliver(item))
            except RuntimeError:
                if self._loop is not None:
                    asyncio.run_coroutine_threadsafe(self._deliver(item), self._loop)
        except Exception:
            logger.exception("node %s (MQTTInputNode): failed to handle incoming message", self.id)

    async def _deliver(self, item):
        self.emit('out', item)

    def _conn_key(self):
        """Snapshot of every config value that affects the live connection
        - used to detect a live attribute-wire update so process() can
        actually reconnect, instead of the old behavior where broker/port/
        topic/qos/username/password were only ever read once at connect
        time (inside _connect_client(), invoked once from process()) and a
        live update to any of them had zero effect on the already-running
        client for the rest of the node's life."""
        return (self.broker, self.port, self.topic, self.qos, self.username, self.password)

    def _connect_client(self):
        client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)
        if self.username and self.password:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect
        client.on_message = self._on_message

        broker, port = self.broker, self.port
        # Run network loop in thread
        import threading
        def _run():
            client.connect(broker, port, keepalive=60)
            client.loop_forever()
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        self._client = client

    async def process(self):
        loop = asyncio.get_running_loop()
        self._loop = loop
        self._connect_client()

        # Wait for connection
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
        self._connected_key = self._conn_key()

        while self._running:
            await asyncio.sleep(1)
            # A live attribute-wire update to broker/port/topic/qos/
            # username/password used to be silently ineffective forever -
            # detect the change here and reconnect with the new values
            # instead of requiring a manual node restart.
            live_key = self._conn_key()
            if live_key != self._connected_key:
                old_client = self._client
                self._connected.clear()
                self._connect_client()
                try:
                    await asyncio.wait_for(self._connected.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass
                self._connected_key = self._conn_key()
                if old_client is not None:
                    try:
                        old_client.disconnect()
                    except Exception:
                        logger.exception(
                            "node %s (MQTTInputNode): error disconnecting old client after live reconnect",
                            self.id,
                        )

    async def stop(self):
        await super().stop()
        if self._client:
            self._client.disconnect()
