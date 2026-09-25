from ..core.transform_node import SingleInputTransformNode


class NumericMulNode(SingleInputTransformNode):
    async def configure(self):
        self.value = float(self.config.get('value', 1))

    def transform(self, item):
        return float(item) * self.value
