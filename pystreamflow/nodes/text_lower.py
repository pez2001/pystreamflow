from ..core.transform_node import SingleInputTransformNode


class TextLowerNode(SingleInputTransformNode):
    def transform(self, item):
        return str(item).lower()
