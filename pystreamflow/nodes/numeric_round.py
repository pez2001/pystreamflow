from ..core.transform_node import SingleInputTransformNode


class NumericRoundNode(SingleInputTransformNode):
    async def configure(self):
        self.ndigits = int(self.config.get('ndigits', 0))

    def transform(self, item):
        return round(float(item), self.ndigits)
