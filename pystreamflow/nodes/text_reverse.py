from ..core.transform_node import SingleInputTransformNode


class TextReverseNode(SingleInputTransformNode):
    def transform(self, item):
        return str(item)[::-1]
