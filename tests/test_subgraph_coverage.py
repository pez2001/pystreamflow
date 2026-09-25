"""
Coverage for pystreamflow/nodes/subgraph.py (SubgraphNode) - previously
11% covered (97 of 109 statements missed), the second-biggest coverage
gap in the project after mcp/server.py. SubgraphNode embeds an entire
internal workflow (its own node instances and pipes, built from the same
core/registry.py source of truth Engine/API/MCP use) inside a single
node, bridging one external input port and one external output port to
an "entry" and "exit" node inside that internal graph.

These tests drive real data end-to-end through a real (if tiny) internal
graph, rather than only checking that construction doesn't crash.
"""
import asyncio

import pytest

from pystreamflow.core.stream import Pipe
from pystreamflow.nodes.subgraph import SubgraphNode


async def _run_subgraph(config, input_item, timeout=3.0):
    node = SubgraphNode('sg', config)
    inp, out = Pipe(), Pipe()
    node.add_input('in', inp)
    node.add_output('out', out)
    await node.start()
    try:
        await inp.put(input_item)
        return await asyncio.wait_for(out.get(), timeout=timeout)
    finally:
        await node.stop()


SINGLE_NODE_YAML = """
nodes:
  - id: up
    type: TextUpperNode
    config: {}
edges: []
"""

CHAIN_YAML = """
nodes:
  - id: up
    type: TextUpperNode
    config: {}
  - id: trim
    type: TextTrimNode
    config: {}
edges:
  - source: up
    target: trim
    source_port: out
    target_port: in
    type: data
"""


async def test_subgraph_single_node_transforms_data_end_to_end():
    result = await _run_subgraph({'workflow_yaml': SINGLE_NODE_YAML}, '  hello  ')
    assert result == '  HELLO  '


async def test_subgraph_chained_nodes_transform_data_end_to_end():
    result = await _run_subgraph({'workflow_yaml': CHAIN_YAML}, '  hello  ')
    # TextUpperNode -> TextTrimNode: uppercase first, then strip whitespace.
    assert result == 'HELLO'


async def test_subgraph_from_workflow_path(tmp_path):
    wf_path = tmp_path / 'inner.yaml'
    wf_path.write_text(CHAIN_YAML)
    result = await _run_subgraph({'workflow_path': str(wf_path)}, '  hi  ')
    assert result == 'HI'


async def test_subgraph_requires_workflow_source():
    node = SubgraphNode('sg', {})
    with pytest.raises(ValueError, match='requires workflow_path or workflow_yaml'):
        await node.init()


async def test_subgraph_unknown_node_type_falls_back_to_generic_node():
    yaml_src = """
nodes:
  - id: mystery
    type: ThisNodeTypeDoesNotExist
    config: {}
edges: []
"""
    node = SubgraphNode('sg', {'workflow_yaml': yaml_src})
    await node.init()
    assert node.internal_nodes['mystery'].__class__.__name__ == 'GenericNode'


async def test_subgraph_explicit_input_and_output_node_ids():
    # Force entry/exit to specific nodes instead of the auto-detected
    # "not a target"/"not a source" defaults, exercising the
    # input_node_id/output_node_id config branches.
    result = await _run_subgraph({
        'workflow_yaml': CHAIN_YAML,
        'input_node_id': 'up',
        'output_node_id': 'trim',
    }, '  hey  ')
    assert result == 'HEY'


async def test_subgraph_stop_cancels_internal_tasks():
    node = SubgraphNode('sg', {'workflow_yaml': SINGLE_NODE_YAML})
    inp = Pipe()
    node.add_input('in', inp)
    await node.start()
    await asyncio.sleep(0.05)
    assert node._internal_tasks
    await node.stop()
    await asyncio.sleep(0.05)
    assert all(t.cancelled() or t.done() for t in node._internal_tasks)


TWO_INDEPENDENT_NODES_YAML = """
nodes:
  - id: up
    type: TextUpperNode
    config: {}
  - id: trim
    type: TextTrimNode
    config: {}
edges: []
"""


async def test_subgraph_multi_input_output_bridges():
    # Regression coverage for the new input_bridges/output_bridges config
    # (Request C10: "subgraphs should be able to use multiple inputs and
    # outputs"). Two internal nodes, each independently bridged to its own
    # external input and output port - not chained to each other at all -
    # confirming both bridges are wired and pumped concurrently and
    # neither cross-talks with the other.
    node = SubgraphNode('sg', {
        'workflow_yaml': TWO_INDEPENDENT_NODES_YAML,
        'input_bridges': [
            {'external_port': 'inA', 'node_id': 'up'},
            {'external_port': 'inB', 'node_id': 'trim'},
        ],
        'output_bridges': [
            {'external_port': 'outA', 'node_id': 'up'},
            {'external_port': 'outB', 'node_id': 'trim'},
        ],
    })
    inA, inB = Pipe(), Pipe()
    outA, outB = Pipe(), Pipe()
    node.add_input('inA', inA)
    node.add_input('inB', inB)
    node.add_output('outA', outA)
    node.add_output('outB', outB)
    await node.start()
    try:
        await inA.put('  hello  ')
        await inB.put('  hey  ')
        resultA = await asyncio.wait_for(outA.get(), timeout=3.0)
        resultB = await asyncio.wait_for(outB.get(), timeout=3.0)
        assert resultA == '  HELLO  '  # TextUpperNode: case-only, whitespace kept
        assert resultB == 'hey'        # TextTrimNode: whitespace-only, case kept
    finally:
        await node.stop()


async def test_subgraph_legacy_single_bridge_config_still_works_unchanged():
    # input_node_id/output_node_id (no input_bridges/output_bridges at
    # all) must produce exactly one bridge each, identical to the
    # pre-multi-bridge behavior - this is the backward-compatibility
    # guarantee the new list-based config is required to preserve.
    node = SubgraphNode('sg', {
        'workflow_yaml': CHAIN_YAML,
        'input_node_id': 'up',
        'output_node_id': 'trim',
    })
    await node.init()
    assert len(node._input_bridges) == 1
    assert len(node._output_bridges) == 1
    assert node._input_bridges[0][0] == 'in'
    assert node._output_bridges[0][0] == 'out'
    assert node.entry_node is node.internal_nodes['up']
    assert node.exit_node is node.internal_nodes['trim']


async def test_subgraph_idles_without_external_input_wired():
    # process() has a "wait for the external input port to be connected"
    # branch when nothing wired self.input_port before start(); confirm
    # it doesn't crash or busy-loop forever, and stop() still works.
    node = SubgraphNode('sg', {'workflow_yaml': SINGLE_NODE_YAML})
    await node.start()
    await asyncio.sleep(0.05)
    await node.stop()
