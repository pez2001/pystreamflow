import asyncio
from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.mqtt_input import MQTTInputNode
from pystreamflow.nodes.mqtt_output import MQTTOutputNode
from pystreamflow.nodes.scripted_output import ScriptedOutputNode
from pystreamflow.nodes.web_output import WebOutputNode
from pystreamflow.nodes.web_output_json import WebOutputJSONNode

async def test_scripted_output():
    # Regression note: this test used to read `node.inputs['in']`
    # straight after init() with nothing having ever wired that pipe -
    # unlike WebOutputNode/WebOutputJSONNode below, ScriptedOutputNode's
    # init() does not self-create an 'in' pipe (most node types rely on
    # the caller/engine to add_input() one), so that raised KeyError('in')
    # every time. It also asserted `last[0]['output']`, but
    # BaseNode.get_last() wraps every emitted item as
    # `{'port': ..., 'item': ...}` (see core/node.py's emit()), so even
    # with the pipe wired that assertion shape was wrong too - it never
    # ran far enough to hit it. Fixed by wiring the input explicitly
    # (the standard pattern used throughout test_nodes_coverage.py) and
    # asserting the correct shape.
    node = ScriptedOutputNode('test_script', {'script': 'return data * 2'})
    await node.init()
    inp = Pipe()
    node.add_input('in', inp)
    await node.start()
    await inp.put(5)
    await asyncio.sleep(0.2)
    last = node.get_last(1)
    assert last, 'no output'
    assert last[0]['item']['output'] == 10
    await node.stop()
    print('scripted_output ok')

async def test_web_output():
    node = WebOutputNode('test_web', {'path': '/test'})
    await node.init()
    await node.start()
    pipe = node.inputs['in']
    await pipe.put('hello')
    await asyncio.sleep(0.1)
    last = node.get_last(1)
    assert last and last[0]['item'] == 'hello'
    await node.stop()
    print('web_output ok')

async def test_web_output_json():
    node = WebOutputJSONNode('test_webjson', {})
    await node.init()
    await node.start()
    pipe = node.inputs['in']
    await pipe.put({'a':1})
    await asyncio.sleep(0.1)
    last = node.get_last(1)
    assert last
    await node.stop()
    print('web_output_json ok')

if __name__ == '__main__':
    asyncio.run(test_scripted_output())
    asyncio.run(test_web_output())
    asyncio.run(test_web_output_json())
    print('new nodes tests passed')
