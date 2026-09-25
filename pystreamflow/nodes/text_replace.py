from ..core.transform_node import SingleInputTransformNode


class TextReplaceNode(SingleInputTransformNode):
    async def configure(self):
        self.find = self.config.get('find', '')
        self.replace = self.config.get('replace', '')

    def transform(self, item):
        return str(item).replace(self.find, self.replace)
