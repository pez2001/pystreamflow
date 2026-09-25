from ..core.transform_node import SingleInputTransformNode


class TextStripNode(SingleInputTransformNode):
    async def configure(self):
        self.chars = self.config.get('chars', None)

    def transform(self, item):
        s = str(item)
        return s.strip(self.chars) if self.chars else s.strip()
