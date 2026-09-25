import importlib.util
import inspect
import pkgutil
import pathlib
from typing import Dict, Type

from .node import BaseNode

class PluginManager:
    def __init__(self, plugins_dir: pathlib.Path):
        self.plugins_dir = plugins_dir
        self.registry: Dict[str, Type] = {}

    def load_plugins(self):
        if not self.plugins_dir.exists():
            return
        for finder, name, ispkg in pkgutil.iter_modules([str(self.plugins_dir)]):
            spec = importlib.util.spec_from_file_location(name, self.plugins_dir / f"{name}.py")
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                for attr in dir(module):
                    obj = getattr(module, attr)
                    # Only register concrete BaseNode subclasses *defined in
                    # this plugin module* - not BaseNode itself, not other
                    # abstract subclasses, and not classes merely imported
                    # into the module's namespace (e.g. `BaseNode` itself,
                    # which every plugin imports and which previously got
                    # registered as if it were a creatable node type named
                    # "BaseNode").
                    if (
                        isinstance(obj, type)
                        and issubclass(obj, BaseNode)
                        and obj is not BaseNode
                        and not inspect.isabstract(obj)
                        and obj.__module__ == module.__name__
                    ):
                        self.registry[obj.__name__] = obj

    def get_node_class(self, name: str):
        return self.registry.get(name)

    def list_nodes(self):
        return list(self.registry.keys())
