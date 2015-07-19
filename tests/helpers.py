from asyncio import base_events
import asyncio
import io
import struct

from magician import wire


class Prototype(object):
    """Similar to a class in prototypical inheritance."""
    pass


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
        self.closed = False
        self.close_callback = close_callback
        self.close_args = args

    def close(self):
        self.closed = True
        self.close_callback(*self.close_args)


class FrameReceiver(object):
    """Acts like a stream writer and knows a little about AMQP."""

    def __init__(self):
        super(FrameReceiver, self).__init__()
        self._buffer = io.BytesIO()
        self._frames = []

    def write(self, buffer):
        self._buffer.write(buffer)

    @property
    def frames(self):
        """List of decoded frames."""
        if self._buffer.tell() != 0:
            self._decode_frames()
        return self._frames

    def _decode_frames(self):
        """Decode frames from the stream if there are any."""

        # If the first thing in the stream is the header, then
        # skip it.  This will happen the first time that we read
        # frames from the stream.
        self._buffer.seek(0)
        if self._buffer.read(4) == b'AMQP':
            self._buffer.seek(8)
        else:
            self._buffer.seek(0)

        while True:
            hdr = self._buffer.read(7)
            if not hdr:
                break

            frame_type, channel, frame_size = struct.unpack('>BHI', hdr)
            frame_body = self._buffer.read(frame_size)
            frame_end = self._buffer.read(1)
            assert frame_end == b'\xCE'

            new_frame = {
                'type': frame_type,
                'channel': channel,
                'body': Prototype(),
            }
            body = new_frame['body']
            body.raw = frame_body
            if frame_type == wire.Frame.METHOD:
                body.class_id, body.method_id = struct.unpack(
                    '>HH', frame_body[:4])
                body.method_body = frame_body[4:]
            self._frames.append(new_frame)

        self._buffer = io.BytesIO()
