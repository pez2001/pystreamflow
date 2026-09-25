from pystreamflow.core.models import Graph, Node, Edge
from pystreamflow.core.persistence import save_workflow, load_workflow
import tempfile, os

def test_save_load():
    g = Graph()
    g.add_node(Node(id='n1', type='A', config={'x':1}))
    g.add_edge(Edge(source='n1', target='n2'))
    with tempfile.NamedTemporaryFile(delete=False, suffix='.yaml') as f:
        path = f.name
    save_workflow(g, path)
    g2 = load_workflow(path)
    assert len(g2.nodes) == 1
    assert g2.nodes[0].id == 'n1'
    os.unlink(path)

if __name__ == '__main__':
    test_save_load()
    print('persistence tests passed')
