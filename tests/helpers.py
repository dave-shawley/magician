import io


class AsyncBufferReader(object):
    """Simple implementation of asyncio.StreamReader over a buffer."""

    def __init__(self, buffer):
        super(AsyncBufferReader, self).__init__()
        self.buffer = buffer
        self.stream = io.BytesIO(self.buffer)

    @asyncio.coroutine
    def read(self, num_bytes):
        return self.stream.read(num_bytes)
