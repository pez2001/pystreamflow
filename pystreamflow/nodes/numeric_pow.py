from ..core.transform_node import SingleInputTransformNode


class NumericPowNode(SingleInputTransformNode):
    async def configure(self):
        self.value = float(self.config.get('value', 2))

    def transform(self, item):
        return float(item) ** self.value
