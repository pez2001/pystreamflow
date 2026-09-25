from ..core.transform_node import SingleInputTransformNode


class TextUpperNode(SingleInputTransformNode):
    def transform(self, item):
        return str(item).upper()
