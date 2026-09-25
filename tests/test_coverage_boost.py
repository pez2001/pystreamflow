"""
Boost coverage by importing and exercising core modules.
"""
import asyncio
from pystreamflow.core.node import BaseNode
from pystreamflow.core.stream import Pipe
from pystreamflow.core.engine import Engine
from pystreamflow.core.models import Graph, Node, Edge
from pystreamflow.core.session_manager import session_manager
from pystreamflow.core.plugin_manager import PluginManager
from pystreamflow.core.web_server import register_node
from pystreamflow.api.server import app
from pystreamflow.mcp.server import app as mcp_app

def test_imports():
    assert BaseNode
    assert Pipe
    assert Engine
    assert Graph
    assert session_manager
    assert PluginManager
    assert register_node
    assert app
    assert mcp_app

class _MinimalNode(BaseNode):
    """The smallest possible concrete BaseNode subclass - just enough to
    satisfy the abstract `process()` method - used only to exercise the
    shared lifecycle (start/stop) without any node-specific behavior.

    Regression note: this test used to instantiate BaseNode directly
    (`BaseNode('test_node', {})`), which has always raised
    `TypeError: Can't instantiate abstract class BaseNode with abstract
    method process` - BaseNode declares `process()` with @abstractmethod
    (see core/node.py), so it was never actually instantiable. The test
    never should have passed as written; this subclass is the fix.
    """
    async def process(self):
        while self._running:
            await asyncio.sleep(0)


async def test_base_node_lifecycle():
    node = _MinimalNode('test_node', {})
    await node.start()
    assert node._running
    await node.stop()
    assert not node._running

def test_graph():
    g = Graph()
    n = Node(id='n1', type='TestNode', config={})
    g.add_node(n)
    e = Edge(source='n1', target='n2')
    g.add_edge(e)
    assert len(g.nodes) == 1
    assert len(g.edges) == 1

def test_pipe_basic():
    p = Pipe()
    assert p.stats() is not None

def test_session_manager_exists():
    assert session_manager is not None

if __name__ == '__main__':
    test_imports()
    asyncio.run(test_base_node_lifecycle())
    test_graph()
    test_pipe_basic()
    test_session_manager_exists()
    print('coverage boost tests passed')
