from asyncio import base_events
import asyncio
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

    def exception(self):
        return None

    def set_transport(self):
        pass


class FakeEventLoop(base_events.BaseEventLoop):

    @asyncio.coroutine
    def create_connection(self, protocol_factory, host=None, port=None, *,
                          ssl=None, family=0, proto=0, flags=0, sock=None,
                          local_addr=None, server_hostname=None):
        return object(), protocol_factory()


class FakeTransport(asyncio.Transport):

    def __init__(self, close_callback, *args):
        super(FakeTransport, self).__init__()
        self.close_callback = close_callback
        self.close_args = args

    def close(self):
        self.close_callback(*self.close_args)
