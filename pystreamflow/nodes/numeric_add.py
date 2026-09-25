from ..core.transform_node import SingleInputTransformNode


class NumericAddNode(SingleInputTransformNode):
    async def configure(self):
        self.value = float(self.config.get('value', 0))

    def transform(self, item):
        return float(item) + self.value
