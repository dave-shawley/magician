from asyncio import base_events
import asyncio
import io
import struct

from magician import wire


class AsyncBufferReader(object):
    """Simple implementation of asyncio.StreamReader over a buffer."""

    def __init__(self):
        super(AsyncBufferReader, self).__init__()
        self.stream = io.BytesIO()

    def add_method_frame(self, class_id, method_id, channel, *buffers):
        """
        Add a method frame.

        :param int class_id: ID of the class that is being invoked
        :param int method_id: ID of the method that is being invoked
        :param int channel: channel that this frame is associated with
        :param buffers: arbitrary number of byte buffers to append

        """
        payload_size = sum(len(buffer) for buffer in buffers)
        iobuf = io.BytesIO()
        iobuf.write(struct.pack('>BHI', wire.Frame.METHOD, channel,
                                payload_size + 4))
        iobuf.write(struct.pack('>HH', class_id, method_id))
        self.stream.write(iobuf.getvalue())
        for buf in buffers:
            self.stream.write(buf)
        self.stream.write(wire.Frame.END_BYTE)

    def add_buffers(self, *buffers):
        """Append an arbitrary number of byte buffers to the stream."""
        for buffer in buffers:
            self.stream.write(buffer)

    def rewind(self):
        self.stream.seek(0)

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
