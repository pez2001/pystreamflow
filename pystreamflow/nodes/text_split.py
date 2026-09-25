from ..core.transform_node import SingleInputTransformNode


class TextSplitNode(SingleInputTransformNode):
    async def configure(self):
        self.sep = self.config.get('sep', ' ')
        self.maxsplit = self.config.get('maxsplit', -1)

    def transform(self, item):
        return str(item).split(self.sep, self.maxsplit)
