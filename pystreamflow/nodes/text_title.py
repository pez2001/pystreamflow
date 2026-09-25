from ..core.transform_node import SingleInputTransformNode


class TextTitleNode(SingleInputTransformNode):
    def transform(self, item):
        return str(item).title()
