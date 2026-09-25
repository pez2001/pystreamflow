from ..core.transform_node import SingleInputTransformNode


class NumericMinNode(SingleInputTransformNode):
    async def configure(self):
        self.value = float(self.config.get('value', 0))

    def transform(self, item):
        return min(float(item), self.value)
