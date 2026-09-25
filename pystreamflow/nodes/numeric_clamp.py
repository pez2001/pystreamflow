from ..core.transform_node import SingleInputTransformNode


class NumericClampNode(SingleInputTransformNode):
    async def configure(self):
        self.min_val = float(self.config.get('min', 0))
        self.max_val = float(self.config.get('max', 100))

    def transform(self, item):
        v = float(item)
        return max(self.min_val, min(self.max_val, v))
