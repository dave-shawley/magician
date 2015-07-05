"""
Low-level AMQP protocol decoding.

Protocol packet decoding primitives are implemented as simple functions
that accept a byte buffer and the offset to start decoding at.  The return
value is the similar, the decoded value and the offset to continue decoding
the remaining packet at.

- :func:`.decode_short_string` parses a short byte string, at most 255 bytes
- :func:`.decode_long_string` parses a byte string of up to 2^32 bytes
- :func:`.decode_table` parses a field table into a ``dict`` with byte
  strings as keys

The AMQP protocol elements are represented as class instances.  The
:class:`.Frame` class is a top-level frame returned from the
:func:`.read_frame` coroutine.  It's ``body`` attribute is an instance of
a *payload class*.  Each of the payload classes implement a ``from_bytes``
class method that decode instances from a byte string.

- :class:`.Frame` represents a top-level frame.  The ``body`` attribute
  is a instance of the appropriate payload class.
- :class:`.Connection` represents a *Connection class* frame.

"""
import asyncio
import logging
import struct

from . import errors


LOGGER = logging.getLogger(__name__)


def decode_table(data, offset):
    """
    Decode a field table into a ``dict``

    :param bytes data: buffer to decode from
    :param int offset: offset into `data` to start decoding at
    :return: a ``tuple`` of the decoded table as a ``dict`` and
        the integer offset into `data` that follows the table
    :raises: :class:`~magician.errors.ProtocolFailure` if an
        unhandled field type is encountered

    """
    table = {}
    table_size = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    end_offset = offset + table_size
    while offset < end_offset:
        field_name, offset = decode_short_string(data, offset)
        field_type = chr(data[offset])
        offset += 1
        if field_type == 'S':
            field_value, offset = decode_long_string(data, offset)
        elif field_type == 'F':
            field_value, offset = decode_table(data, offset)
        elif field_type == 't':
            field_value, offset = bool(data[offset]), offset + 1
        else:
            raise errors.ProtocolFailure('unknown field type {0}',
                                         field_type)
        table[field_name] = field_value
    return table, offset


def decode_short_string(data, offset):
    """
    Decode a short byte string.

    :param bytes data: buffer to decode from
    :param int offset: offset into `data` to start decoding at
    :return: a ``tuple`` of the decoded string as a ``bytes``
        instance and the integer offset into `data` that follows
        the decoded string

    """
    string_length = data[offset]
    offset += 1
    string_data = bytes(data[offset:offset+string_length])
    return string_data, offset + string_length


def decode_long_string(data, offset):
    """
    Decode a byte string of arbitrary length.

    :param bytes data: buffer to decode from
    :param int offset: offset into `data` to start decoding at
    :return: a ``tuple`` of the decoded string as a ``bytes``
        instance and the integer offset into `data` that follows
        the decoded string

    """
    string_length = struct.unpack('>I', data[offset:offset+4])[0]
    offset += 4
    string_data = bytes(data[offset:offset+string_length])
    return string_data, offset + string_length


class Frame(object):
    """
    Decoded frame instance.

    Frames are decoded by the :func:`.read_frame` co-routine. They
    can be created manually by calling the initializer for this class
    with the appropriate byte string.

    :param int frame_type: AMQP frame type.  This should be a value
        between one and four inclusively.
    :param int channel: channel that the frame is associated with.
    :param bytes body: payload of the frame

    :raises: :class:`magician.errors.ProtocolFailure` if the frame
        cannot be decoded

    """
    END_BYTE = b'\xCE'
    METHOD = 1
    HEADER = 2
    BODY = 3
    HEARTBEAT = 4

    def __init__(self, frame_type, channel, body):
        super(Frame, self).__init__()
        if frame_type not in (1, 2, 3, 4):
            raise errors.ProtocolFailure('invalid frame type ({0}) received',
                                         frame_type)

        self.frame_type = frame_type
        self.channel = channel
        self.raw_body = body
        LOGGER.debug('BODY %r', body)

        if self.frame_type == self.METHOD:
            self._decode_method()

    def _decode_method(self):
        view = memoryview(self.raw_body)
        class_id = (view[0] << 8) | view[1]
        if class_id == Connection.CLASS_ID:
            self.body = Connection.from_bytes(view[2:])

    def __repr__(self):
        return (
            '<magician.wire.Frame: type={0.frame_type} channel={0.channel} '
            '{1} body bytes>'.format(self, len(self.raw_body)))


class Connection(object):
    CLASS_ID = 10

    class Methods(object):
        START = 10

    @classmethod
    def from_bytes(cls, data):
        self = cls()
        self.class_id = cls.CLASS_ID
        self.method_id = (data[0] << 8) | data[1]
        if self.method_id == self.Methods.START:
            self.version_major, self.version_minor = data[2], data[3]
            self.server_properties, offset = decode_table(data, 4)
            mechanisms, offset = decode_long_string(data, offset)
            self.security_mechanisms = mechanisms.split()
            locales, offset = decode_long_string(data, offset)
            self.locales = locales.split()
        else:
            raise errors.ProtocolFailure('unknown connection method {0}',
                                         self.method_id)
        return self


@asyncio.coroutine
def read_frame(reader):
    """
    Read a frame from an async reader and return the decoded instance.

    :param asyncio.StreamReader reader: instance to read data from
    :return: decoded information as a :class:`.Frame` instance
    :raise: :class:`magician.errors.ProtocolFailure`

    """
    frame_header = yield from reader.read(7)
    LOGGER.debug('received frame header %r', frame_header)

    frame_type, channel, frame_size = struct.unpack('>BHI', frame_header)
    frame_body = yield from reader.read(frame_size)
    frame_end = yield from reader.read(1)
    if frame_end != Frame.END_BYTE:
        raise errors.ProtocolFailure('invalid frame end ({0:02x}) received',
                                     frame_end)

    frame = Frame(frame_type, channel, frame_body)
    if frame.frame_type == Frame.HEARTBEAT and frame.channel != 0:
        raise errors.ProtocolFailure('heartbeat received for channel {0}',
                                     channel)

    return frame
