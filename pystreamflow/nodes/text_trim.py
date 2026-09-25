from ..core.transform_node import SingleInputTransformNode


class TextTrimNode(SingleInputTransformNode):
    def transform(self, item):
        return str(item).strip()
