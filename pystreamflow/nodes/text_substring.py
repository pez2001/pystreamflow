from ..core.transform_node import SingleInputTransformNode


class TextSubstringNode(SingleInputTransformNode):
    # NOT self.start/self.end - BaseNode already defines a start() lifecycle
    # method, and the original pre-refactor code (`self.start = int(...)`)
    # silently shadowed it with a plain int on every instance. That's
    # invisible the first time start() runs (Python had already resolved
    # the bound method before init() ran inside it), but any LATER call to
    # node.start() - e.g. a Pipe auto-starting its owning node the first
    # time something reads from it, or a pause/resume cycle - would then
    # hit "TypeError: 'int' object is not callable" instead of actually
    # starting the node. Found while verifying this file's Phase 4 dedup
    # (see core/transform_node.py); fixed here since it predates the
    # refactor and would have hit any caller that starts a
    # TextSubstringNode indirectly rather than by an explicit start() call
    # taken before init() has run.
    async def configure(self):
        self.start_idx = int(self.config.get('start', 0))
        end = self.config.get('end')
        self.end_idx = int(end) if end is not None else None

    def transform(self, item):
        return str(item)[self.start_idx:self.end_idx]
