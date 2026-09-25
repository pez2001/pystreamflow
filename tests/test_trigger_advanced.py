"""
Tests for advanced trigger nodes
"""
import asyncio
from pystreamflow.nodes.trigger_advanced import TriggerIfNode, TriggerThresholdNode, TriggerDebounceNode, TriggerPulseNode, TriggerToggleNode
from pystreamflow.core.stream import Pipe
from pystreamflow.core.web_server import _nodes

async def test_trigger_if():
    n = TriggerIfNode('tif', {'condition':'truthy','action':'start','target_node_id':'tgt'})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('data')
    await asyncio.sleep(0.05)
    await n.stop()

async def test_trigger_threshold():
    n = TriggerThresholdNode('thresh', {'threshold':2,'action':'start','target_node_id':'tgt','reset':True})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put(1)
    await inp.put(2)
    await asyncio.sleep(0.05)
    await n.stop()

async def test_trigger_debounce():
    n = TriggerDebounceNode('deb', {'debounce':0.05,'action':'start','target_node_id':'tgt'})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put('x')
    await asyncio.sleep(0.15)
    await n.stop()

async def test_trigger_pulse():
    n = TriggerPulseNode('pulse', {'interval':0.02,'pulse_count':2,'action':'start','target_node_id':'tgt'})
    out = Pipe()
    n.add_output('out', out)
    await n.start()
    await asyncio.sleep(0.1)
    await n.stop()

async def test_trigger_toggle():
    n = TriggerToggleNode('tog', {'action_on':'start','action_off':'stop','target_node_id':'tgt'})
    inp = Pipe()
    out = Pipe()
    n.add_input('in', inp)
    n.add_output('out', out)
    await n.start()
    await inp.put(1)
    await inp.put(2)
    await asyncio.sleep(0.05)
    await n.stop()

if __name__ == '__main__':
    async def run():
        await test_trigger_if()
        await test_trigger_threshold()
        await test_trigger_debounce()
        await test_trigger_pulse()
        await test_trigger_toggle()
        print('advanced trigger tests passed')
    asyncio.run(run())
