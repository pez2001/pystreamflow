from pystreamflow.core.plugin_manager import PluginManager
import pathlib

def test_plugin_load():
    pm = PluginManager(pathlib.Path('pystreamflow/plugins'))
    pm.load_plugins()
    # example plugin should be loaded
    assert 'ExamplePluginNode' in pm.list_nodes()

if __name__ == '__main__':
    test_plugin_load()
    print('plugin tests passed')
