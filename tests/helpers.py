from asyncio import base_events
import asyncio
import contextlib
import io
import struct

from magician import wire


class Prototype(object):
    """Similar to a class in prototypical inheritance."""
    pass


class AsyncBufferReader(object):
    """
    Implementation of asyncio.StreamReader over a buffer.

    This is a little tricky to get right.  Since we need to be able to
    inject frames while the test code is running, there is a Future that
    gets created when it is needed.  We also need to track both the read
    and write offsets so that we can read and write in an overlapped
    manner.

    """

    def __init__(self, emulate_eof=False):
        super(AsyncBufferReader, self).__init__()
        self.stream = io.BytesIO()
        self._offsets = {'read': 0, 'write': 0}
        self._emulate_eof = emulate_eof
        self._wait_frame_available = None

    def add_method_frame(self, class_id, method_id, channel, *buffers):
        """
        Add a method frame.

        :param int class_id: ID of the class that is being invoked
        :param int method_id: ID of the method that is being invoked
        :param int channel: channel that this frame is associated with
        :param buffers: arbitrary number of byte buffers to append

        """
        self.add_frame(wire.Frame.METHOD, channel,
                       struct.pack('>HH', class_id, method_id), *buffers)

    def add_frame(self, frame_type, channel, *body):
        """
        Add a complete frame.

        :param int frame_type: AMQP frame type
        :param int channel: channel that the frame is assocatied with
        :param body: frame body omitting the end byte

        """
        frame = io.BytesIO()
        payload_size = sum(len(chunk) for chunk in body)
        frame.write(struct.pack('>BHI', frame_type, channel, payload_size))
        for chunk in body:
            frame.write(chunk)
        frame.write(wire.Frame.END_BYTE)
        self.emit_frame(frame.getvalue())

    def emit_frame(self, frame):
        """
        Emit a frame.

        :param bytes frame: assembled frame to emit

        If the someone is currently wiating for a frame, then delivery
        it using the future; otherwise, write it to the output stream.
        Note that this method **does not** require a valid frame so you
        can use it to inject malformed frames into the stream.

        """
        with self.maintain_offset('write'):
            self.stream.write(frame)
        if self._wait_frame_available is not None:
            self._wait_frame_available.set_result(None)

    @asyncio.coroutine
    def read(self, num_bytes):
        with self.maintain_offset('read'):
            buf = self.stream.read(num_bytes)

        if not buf and not self._emulate_eof:
            self._wait_frame_available = asyncio.Future()
            yield from self._wait_frame_available
            self._wait_frame_available = None
            buf = yield from self.read(num_bytes)

        return buf

    def exception(self):
        return None

    def set_transport(self):
        pass

    def at_eof(self):
        return (self._emulate_eof and
                self._offsets['read'] == self._offsets['write'])

    @contextlib.contextmanager
    def maintain_offset(self, offset_name):
        self.stream.seek(self._offsets[offset_name])
        yield
        self._offsets[offset_name] = self.stream.tell()


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
        self.frame_available = asyncio.Future()

    def write(self, buffer):
        self._buffer.write(buffer)
        if wire.Frame.END_BYTE in buffer:
            if not self.frame_available.done():
                self.frame_available.set_result(True)

    def clear(self):
        if not self.frame_available.done():
            self.frame_available.cancel()
        self.frame_available = asyncio.Future()
        self._buffer.seek(0)
        self._buffer.truncate(0)

    @property
    def frames(self):
        """List of decoded frames."""
        if self._buffer.tell() != 0:
            self._decode_frames()
            if not self.frame_available.done():
                self.frame_available.set_result(True)
                self.frame_available = asyncio.Future()
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
