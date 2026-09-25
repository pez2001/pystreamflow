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

if __name__ == '__main__':
    asyncio.run(test_validate_cycle())
    asyncio.run(test_validate_ok())
    print('engine tests passed')
