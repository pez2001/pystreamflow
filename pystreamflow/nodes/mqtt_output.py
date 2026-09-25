from ..core.node import BaseNode
import asyncio
import json
import paho.mqtt.client as mqtt
import threading

class MQTTOutputNode(BaseNode):
    async def init(self):
        self.broker = self.config.get('broker', 'localhost')
        self.port = int(self.config.get('port', 1883))
        self.topic = self.config.get('topic', 'pystreamflow/out')
        self.client_id = self.config.get('client_id', f'pystreamflow_out_{self.id}')
        self.username = self.config.get('username')
        self.password = self.config.get('password')
        self.qos = int(self.config.get('qos', 0))
        self.retain = bool(self.config.get('retain', False))
        self._client = None
        self._connected = asyncio.Event()

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self._connected.set()

    def _conn_key(self):
        # See the identical note in mqtt_input.py: broker/port/username/
        # password used to only ever be read once, at connect time - a
        # live attribute-wire update to any of them had zero effect on the
        # already-open connection for the rest of the node's life.
        return (self.broker, self.port, self.username, self.password)

    async def _connect_client(self):
        client = mqtt.Client(client_id=self.client_id, protocol=mqtt.MQTTv311)
        if self.username and self.password:
            client.username_pw_set(self.username, self.password)
        client.on_connect = self._on_connect

        broker, port = self.broker, self.port
        def _run():
            client.connect(broker, port, keepalive=60)
            client.loop_forever()
        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        self._client = client
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
        self._connected_key = self._conn_key()

    async def process(self):
        self._connected.clear()
        await self._connect_client()

        while self._running:
            # A live attribute-wire update to broker/port/username/
            # password: reconnect instead of silently ignoring it.
            if self._conn_key() != self._connected_key:
                old_client = self._client
                self._connected.clear()
                await self._connect_client()
                if old_client is not None:
                    try:
                        old_client.disconnect()
                    except Exception:
                        pass

            pipe = self.inputs.get('in')
            if not pipe:
                await asyncio.sleep(0.5)
                continue
            try:
                item = await asyncio.wait_for(pipe.get(), timeout=1.0)
                payload = json.dumps(item) if not isinstance(item, str) else item
                self._client.publish(self.topic, payload, qos=self.qos, retain=self.retain)
                # Bug fix (feedback: "mqtt output node's live view is
                # broken and is only showing '[]' even after ... some
                # messages went through"): this node has no real output
                # port at all (port_schema.py declares it a pure sink,
                # same as FileOutputNode) and never called emit() for the
                # data it actually publishes - so GET /nodes/{id}/last
                # could never show anything from normal operation no
                # matter how many messages successfully reached the
                # broker; the only entries that could ever land in
                # self._last_items were incidental attribute-wire-change
                # records (set_attribute()'s own {'port': 'attr:...', ...}
                # bookkeeping), which is exactly consistent with a live
                # view that intermittently shows *something* while
                # otherwise reading empty. record_output() (BaseNode) is
                # the same fix already applied to FileOutputNode for the
                # identical reason.
                self.record_output({'published': item, 'topic': self.topic})
            except asyncio.TimeoutError:
                continue

    async def stop(self):
        await super().stop()
        if self._client:
            self._client.disconnect()
