"""
Coverage for pystreamflow/nodes/mqtt_input.py (MQTTInputNode) and
mqtt_output.py (MQTTOutputNode) - previously 19% covered each. Both talk
to a real MQTT broker via paho.mqtt.client in a background thread, which
these tests replace with an in-process fake client so the node logic
(subscribe-on-connect, JSON-decode-or-passthrough on receive, publish
encoding, disconnect-on-stop) can be exercised without a live broker.

`_on_connect`/`_on_message` are invoked directly from the test's own
coroutine (not from the fake client's background thread) so the fake
client's "network" thread never needs to touch asyncio itself - matching
the project's practice elsewhere of calling a node's own callback methods
directly to test the callback logic in isolation from real I/O.
"""
import asyncio
import time

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.mqtt_input import MQTTInputNode
from pystreamflow.nodes.mqtt_output import MQTTOutputNode


class FakeMQTTClient:
    """Stand-in for paho.mqtt.client.Client: no real network I/O, but
    mirrors the handful of methods/attributes both node types touch."""

    def __init__(self, *args, **kwargs):
        self.on_connect = None
        self.on_message = None
        self.username = None
        self.password = None
        self.subscribed = []
        self.published = []
        self.disconnected = False
        self.connect_called_with = None

    def username_pw_set(self, username, password):
        self.username = username
        self.password = password

    def connect(self, broker, port, keepalive=60):
        self.connect_called_with = (broker, port, keepalive)

    def loop_forever(self):
        # Simulates paho's blocking network loop without ever touching
        # asyncio from this (real OS) thread.
        while not self.disconnected:
            time.sleep(0.02)

    def subscribe(self, topic, qos=0):
        self.subscribed.append((topic, qos))

    def publish(self, topic, payload, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))

    def disconnect(self):
        self.disconnected = True


class FakeMsg:
    def __init__(self, topic, payload, qos=0, retain=False):
        self.topic = topic
        self.payload = payload.encode() if isinstance(payload, str) else payload
        self.qos = qos
        self.retain = retain


@pytest.fixture
def fake_mqtt_input(monkeypatch):
    import pystreamflow.nodes.mqtt_input as mqtt_input_module
    monkeypatch.setattr(mqtt_input_module.mqtt, 'Client', FakeMQTTClient)
    return mqtt_input_module


@pytest.fixture
def fake_mqtt_output(monkeypatch):
    import pystreamflow.nodes.mqtt_output as mqtt_output_module
    monkeypatch.setattr(mqtt_output_module.mqtt, 'Client', FakeMQTTClient)
    return mqtt_output_module


# ---------- MQTTInputNode ----------

async def test_mqtt_input_node_connects_and_subscribes(fake_mqtt_input):
    n = MQTTInputNode('n', {'topic': 'test/#', 'qos': 1,
                             'username': 'u', 'password': 'p'})
    await n.start()
    try:
        await asyncio.sleep(0.05)
        assert n._client is not None
        # Drive the connect ack ourselves (from this coroutine, not the
        # fake client's background thread) to avoid any cross-thread
        # asyncio hazards while still exercising the real on_connect path.
        n._on_connect(n._client, None, None, 0)
        await asyncio.sleep(0.02)
        assert n._connected.is_set()
        assert n._client.subscribed == [('test/#', 1)]
        assert n._client.username == 'u'
        assert n._client.password == 'p'
    finally:
        await n.stop()
        assert n._client.disconnected


async def test_mqtt_input_node_on_connect_failure_does_not_subscribe(fake_mqtt_input):
    n = MQTTInputNode('n', {'topic': 'test/#'})
    await n.start()
    try:
        await asyncio.sleep(0.02)
        n._on_connect(n._client, None, None, 1)  # non-zero rc = failure
        assert not n._connected.is_set()
        assert n._client.subscribed == []
    finally:
        await n.stop()


async def test_mqtt_input_node_on_message_parses_json_payload(fake_mqtt_input):
    n = MQTTInputNode('n', {})
    await n.start()
    try:
        n._on_message(n._client, None, FakeMsg('t/x', '{"a": 1}', qos=1, retain=True))
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item == {'topic': 't/x', 'payload': {'a': 1}, 'qos': 1, 'retain': True}
    finally:
        await n.stop()


async def test_mqtt_input_node_on_message_passes_through_non_json_payload(fake_mqtt_input):
    n = MQTTInputNode('n', {})
    await n.start()
    try:
        n._on_message(n._client, None, FakeMsg('t/y', 'plain text'))
        item = await asyncio.wait_for(n.out_pipe.get(), timeout=2.0)
        assert item['payload'] == 'plain text'
    finally:
        await n.stop()


async def test_mqtt_input_node_stop_disconnects_client(fake_mqtt_input):
    n = MQTTInputNode('n', {})
    await n.start()
    await asyncio.sleep(0.02)
    await n.stop()
    assert n._client.disconnected


# ---------- MQTTOutputNode ----------

async def test_mqtt_output_node_publishes_json_for_non_string_items(fake_mqtt_output):
    n = MQTTOutputNode('n', {'topic': 'out/topic', 'qos': 2, 'retain': True})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await asyncio.sleep(0.02)
        n._on_connect(n._client, None, None, 0)
        await asyncio.sleep(0.02)
        await inp.put({'a': 1})
        for _ in range(30):
            if n._client.published:
                break
            await asyncio.sleep(0.05)
        assert n._client.published == [('out/topic', '{"a": 1}', 2, True)]
    finally:
        await n.stop()
        assert n._client.disconnected


async def test_mqtt_output_node_publishes_raw_string_items_unmodified(fake_mqtt_output):
    n = MQTTOutputNode('n', {'topic': 'out/topic'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await asyncio.sleep(0.02)
        n._on_connect(n._client, None, None, 0)
        await inp.put('already-a-string')
        for _ in range(30):
            if n._client.published:
                break
            await asyncio.sleep(0.05)
        assert n._client.published == [('out/topic', 'already-a-string', 0, False)]
    finally:
        await n.stop()


async def test_mqtt_output_node_idles_with_no_input_wired(fake_mqtt_output):
    n = MQTTOutputNode('n', {})
    await n.start()
    try:
        await asyncio.sleep(0.05)
        assert n._client.published == []
    finally:
        await n.stop()


async def test_mqtt_output_node_records_published_items_for_the_live_view(fake_mqtt_output):
    # Regression test for a real, directly-reported bug: "mqtt output
    # node's live view is broken and is only showing '[]' even after ...
    # some messages went through". MQTTOutputNode is a pure sink
    # (port_schema.py declares it with no output ports at all, same as
    # FileOutputNode) and never called emit() or otherwise recorded what
    # it actually published - get_last() (what GET /nodes/{id}/last
    # returns to the node editor's live view) could only ever come back
    # empty from normal operation, no matter how many messages reached the
    # broker. BaseNode.record_output() is the fix, applied here the same
    # way it already was to FileOutputNode.
    n = MQTTOutputNode('n', {'topic': 'out/topic'})
    inp = Pipe()
    n.add_input('in', inp)
    await n.start()
    try:
        await asyncio.sleep(0.02)
        n._on_connect(n._client, None, None, 0)
        await inp.put({'msg': 'hello world'})
        for _ in range(30):
            if n._client.published:
                break
            await asyncio.sleep(0.05)
        assert n._client.published == [('out/topic', '{"msg": "hello world"}', 0, False)]
        last = n.get_last()
        assert last and last[-1] == {'published': {'msg': 'hello world'}, 'topic': 'out/topic'}
    finally:
        await n.stop()
