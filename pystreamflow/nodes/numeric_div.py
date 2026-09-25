from ..core.transform_node import SingleInputTransformNode


class NumericDivNode(SingleInputTransformNode):
    async def configure(self):
        self.value = float(self.config.get('value', 1))

    def transform(self, item):
        v = float(self.value)
        return float(item) / v if v != 0 else None
