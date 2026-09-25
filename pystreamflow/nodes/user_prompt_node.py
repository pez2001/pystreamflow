from ..core.node import BaseNode
import asyncio

class UserPromptNode(BaseNode):
    async def init(self):
        self.prompt = self.config.get('prompt', 'Enter value:')
        self.timeout = float(self.config.get('timeout', 300.0))
        self.default = self.config.get('default', '')

    async def process(self):
        while self._running:
            # Emit prompt for UI/API visibility
            self.emit('prompt', {'prompt': self.prompt, 'node_id': self.id})

            # Wait for user input on 'user' input port
            user_pipe = self.inputs.get('user')
            if not user_pipe:
                # No user connection, wait for timeout then emit default
                await asyncio.sleep(min(self.timeout, 1.0))
                if self._paused:
                    await asyncio.sleep(0.1)
                    continue
                self.emit('out', self.default)
                continue

            try:
                value = await asyncio.wait_for(user_pipe.get(), timeout=self.timeout)
                self.emit('out', value)
            except asyncio.TimeoutError:
                self.emit('out', self.default)
                continue
