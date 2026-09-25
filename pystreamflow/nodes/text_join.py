from ..core.transform_node import SingleInputTransformNode


class TextJoinNode(SingleInputTransformNode):
    async def configure(self):
        self.sep = self.config.get('sep', ' ')

    def transform(self, item):
        if isinstance(item, (list, tuple)):
            return self.sep.join(str(x) for x in item)
        return str(item)
