import asyncio
from pystreamflow.core.models import Graph, Node, Edge
from pystreamflow.core.engine import Engine

async def test_validate_cycle():
    g = Graph()
    g.add_node(Node(id='a', type='A', config={}))
    g.add_node(Node(id='b', type='B', config={}))
    g.add_edge(Edge(source='a', target='b'))
    g.add_edge(Edge(source='b', target='a'))
    e = Engine(g)
    try:
        e.validate()
        assert False, 'should raise'
    except ValueError:
        pass

async def test_validate_ok():
    g = Graph()
    g.add_node(Node(id='a', type='A', config={}))
    g.add_node(Node(id='b', type='B', config={}))
    g.add_edge(Edge(source='a', target='b'))
    e = Engine(g)
    assert e.validate() is True

async def test_two_edges_between_the_same_nodes_get_separate_pipes():
    # Regression: pipes used to be keyed by (source, target) only, so a
    # second edge between the same two nodes reused the first edge's pipe.
    from pystreamflow.core.web_server import _nodes

    g = Graph()
    g.add_node(Node(id='src2', type='A', config={}))
    g.add_node(Node(id='dst2', type='B', config={}))
    g.add_edge(Edge(source='src2', target='dst2', source_port='out', target_port='in'))
    g.add_edge(Edge(source='src2', target='dst2', source_port='audio', target_port='audio'))
    e = Engine(g)
    e._instantiate_nodes()
    try:
        e._wire_edges()
        src = e.node_instances['src2']
        assert src.outputs['out'][0][0] is not src.outputs['audio'][0][0]
        assert len(e.pipes) == 2
    finally:
        _nodes.pop('src2', None)
        _nodes.pop('dst2', None)

if __name__ == '__main__':
    asyncio.run(test_validate_cycle())
    asyncio.run(test_validate_ok())
    print('engine tests passed')
