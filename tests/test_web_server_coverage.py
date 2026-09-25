"""
Web server coverage tests
"""
import asyncio
from pystreamflow.core.web_server import register_node, _nodes

class Dummy:
    def __init__(self, id):
        self.id = id
    def get_last(self, n):
        return []

def test_register_node():
    _nodes.clear()
    d = Dummy('d1')
    register_node(d)
    assert 'd1' in _nodes
    assert _nodes['d1'] is d

if __name__ == '__main__':
    test_register_node()
    print('web server tests passed')
