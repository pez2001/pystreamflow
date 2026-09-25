from ..core.transform_node import SingleInputTransformNode


class NumericAbsNode(SingleInputTransformNode):
    def transform(self, item):
        return abs(float(item))
