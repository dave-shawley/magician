"""
Low-level AMQP protocol decoding.

This module contains the low-level methods for reading and writing AMQP
packets.  It exposes two high-level functions for asynchronously reading
a frame and writing a frame:

- :func:`.read_frame` this co-routine reads a :class:`.Frame` instance
  from a :class:`asyncio.StreamReader`
- :func:`.write_frame` this function writes a composed frame out on a
  :class:`asyncio.StreamWriter`

Decoded frames returned from :func:`.read_frame` are represented as a
:class:`Frame` instance which contains the frame data.  It contains the
fully decoded frame as it's ``body`` attribute.

- :class:`.Frame` represents a top-level frame
- :class:`.Connection` represents a *Connection class* frame.

Outgoing frames are handled by the AMQP class specific class.  Each of
the classes that represent an AMQP class include class methods to write
frames for some of the AMQP methods.  For example, the
:meth:`.Connection.construct_start_ok` method will write a fully encoded
frame body for the ``Connection.StartOK`` AMQP method.  Each of the methods
are named using ``construct_`` as a prefix and accepts parameters for
whatever properties are required to construct the frame.  The fully encoded
frame is returned as a :class:`bytes` object.

If you need access to the low-level encoding primitives, then they are
available.  Each is implemented as a simple function that accepts two
parameters -- the value to be written and a *writer* to write the value
to.  A *writer* is anything that implements a ``write`` method that accepts
a byte string such as a :class:`io.BytesIO` or :class:`asyncio.StreamWriter`
instance.

- :func:`.encode_short_string` encodes a string of at most 255 bytes
- :func:`.encode_long_string` encodes string of arbitrary length
- :func:`.encode_table` encodes a dictionary as a AMQP table

Protocol packet decoding primitives are also implemented as simple functions
that accept a byte buffer and the offset to start decoding at.  The return
value is the similar, the decoded value and the offset to continue decoding
the remaining packet at.

- :func:`.decode_short_string` parses a short byte string, at most 255 bytes
- :func:`.decode_long_string` parses a byte string of up to 2^32 bytes
- :func:`.decode_table` parses a field table into a ``dict`` with byte
  strings as keys

"""
import asyncio
import collections
import io
import logging
import struct

from . import errors


LOGGER = logging.getLogger(__name__)


def encode_table(table, buffer):
    """
    Encode a table into a stream.

    :param dict table: data to encode into a table
    :param buffer: a stream-like object. Something like :class:`io.BytesIO`
        works well here.
    :raises ValueError: if the table cannot be encoded

    """
    real_buffer = buffer
    if not buffer.seekable():
        buffer = io.BytesIO()
    buffer.write(b'\xFE\xED\xFA\xCE')
    start_position = buffer.tell()
    field_names = sorted(table.keys())
    for field_name in field_names:
        field_value = table[field_name]
        encode_short_string(field_name, buffer)
        if isinstance(field_value, bool):
            buffer.write(b't')
            buffer.write(b'\x01' if field_value else b'\x00')
        elif isinstance(field_value, (str, bytes)):
            buffer.write(b'S')
            encode_long_string(field_value, buffer)
        elif isinstance(field_value, collections.Mapping):
            buffer.write(b'F')
            encode_table(field_value, buffer)
        else:
            raise ValueError('cannot encode {0} for field {1}'.format(
                type(field_value), field_name))

    bytes_written = buffer.tell() - start_position
    view = buffer.getbuffer()
    view[start_position-4:start_position] = struct.pack('>I', bytes_written)
    del view

    if buffer is not real_buffer:
        real_buffer.write(buffer.getvalue())


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
            raise errors.ProtocolFailure('unknown field type {0} '
                                         'at offset {1}', field_type, offset)
        table[field_name] = field_value
    return table, offset


def encode_short_string(value, buffer):
    """
    Encode a short string.

    :param bytes|str value: value to encode.  If the value has a ``encode``
        attribute, then it will be called with a single parameter of
        ``'utf-8'`` before the string is written
    :param buffer: buffer to write the string to.  This value is expected
        to have a ``write`` method.
    :raises ValueError: if ``value`` is longer than 255 bytes

    """
    value = value.encode('utf-8') if hasattr(value, 'encode') else value
    val_len = len(value)
    if val_len >= 256:
        raise ValueError('Cannot encode %d bytes as short string' % val_len)
    buffer.write(bytes([val_len]))
    buffer.write(value)


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
    string_data = data[offset:offset+string_length]
    return string_data, offset + string_length


def encode_long_string(value, buffer):
    """
    Encode a long byte string.

    :param bytes|str value: value to encode.  If the value has a ``encode``
        attribute, then it will be called with a single parameter of
        ``'utf-8'`` before the string is written
    :param buffer: buffer to write the string to.  This value is expected
        to have a ``write`` method.

    """
    value = value.encode('utf-8') if hasattr(value, 'encode') else value
    buffer.write(struct.pack('>I', len(value)))
    buffer.write(value)


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
    string_data = data[offset:offset+string_length]
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

    .. attribute:: frame_type

       The type of AMQP frame that this instance represents.

    .. attribute:: channel

       The channel that this frame is associated with or zero for
       a connection-level frame.

    .. attribute:: body

       The body as a decoded object instance such as a :class:`.Connection`
       instance for a connection-oriented frame.

    """

    END_BYTE = b'\xCE'
    """Byte string that terminates every AMQP packet."""

    METHOD = 1
    HEADER = 2
    BODY = 3
    HEARTBEAT = 8
    FRAME_TYPES = {
        METHOD: 'method',
        HEADER: 'header',
        BODY: 'body',
        HEARTBEAT: 'heartbeat',
    }

    def __init__(self, frame_type, channel, body):
        super(Frame, self).__init__()
        if frame_type not in self.FRAME_TYPES:
            raise errors.ProtocolFailure('invalid frame type ({0}) received',
                                         frame_type)

        self.frame_type = frame_type
        self.channel = channel
        self.body = None
        if self.frame_type == self.METHOD:
            self.body = self._decode_method(body)
        if self.frame_type == self.HEARTBEAT:
            self.body = self._decode_heartbeat(body)

    def _decode_method(self, body):
        view = memoryview(body)
        class_id = (view[0] << 8) | view[1]
        if class_id == Connection.CLASS_ID:
            return Connection.from_bytes(view[2:])

    def _decode_heartbeat(self, body):
        return

    def __repr__(self):
        label = self.FRAME_TYPES.get(self.frame_type, self.frame_type)
        return ('<{0}: type={1} channel={2} body={3}>'.format(
            self.__class__.__name__, label, self.channel, self.body))


class Connection(object):
    """
    Wire-level details for the AMQP Connection Class.

    The properties defined is dependent on the specific method that
    this instance represents.

    - **Connection.Start**: ``version_major``, ``version_minor``,
      ``server_properties``, ``security_mechanisms``, ``locales``
    - **Connection.Secure**: ``challenge``
    - **Connection.Tune**: ``channel_max``, ``frame_max``,
      ``heartbeat_delay``
    - **Connection.OpenOK**: ``known_hosts``

    """
    CLASS_ID = 10

    class Methods(object):
        """Method constants defined for the AMQP Connection class."""
        START, START_OK = 10, 11
        SECURE, SECURE_OK = 20, 21
        TUNE, TUNE_OK = 30, 31
        OPEN, OPEN_OK = 40, 41
        names = {
            START: 'start',
            START_OK: 'start_ok',
            SECURE: 'secure',
            SECURE_OK: 'secure_ok',
            TUNE: 'tune',
            TUNE_OK: 'tune_ok',
            OPEN: 'open',
            OPEN_OK: 'open_ok',
        }

        @classmethod
        def name(cls, value):
            return cls.names.get(value, value)

    @classmethod
    def from_bytes(cls, data):
        """
        Decodes `data` into a new instance.

        :param bytes data: the packet to parse
        :raises magician.errors.ProtocolFailure:
            if `data` cannot be parsed  as a connection class frame

        """
        self = cls()
        self.class_id = cls.CLASS_ID
        self.method_id = (data[0] << 8) | data[1]

        if self.method_id == self.Methods.START:
            self.version_major, self.version_minor = data[2], data[3]
            self.server_properties, offset = decode_table(data, 4)
            mechanisms, offset = decode_long_string(data, offset)
            self.security_mechanisms = bytes(mechanisms).split()
            locales, offset = decode_long_string(data, offset)
            self.locales = bytes(locales).split()
        elif self.method_id == self.Methods.SECURE:
            self.challenge, _ = decode_long_string(data, 2)
        elif self.method_id == self.Methods.TUNE:
            self.channel_max, self.frame_max, self.heartbeat_delay = \
                struct.unpack('>HIH', data[2:10])
        elif self.method_id == self.Methods.OPEN_OK:
            self.known_hosts = decode_short_string(data, 2)
        else:
            raise errors.ProtocolFailure('unknown connection method {0}',
                                         self.Methods.name(self.method_id))
        return self

    @classmethod
    def construct_start_ok(cls, client_properties, auth_mechanism, auth_value,
                           locale):
        """
        Construct a ``Connection.StartOK`` frame.

        :param dict client_properties: properties to declare to the
            server for this connection
        :param str auth_mechanism: authorization mechanism to employ
        :param str auth_value: authorization data to pass to the server
        :param str locale: message locale to select
        :returns: the encode frame as a :class:`bytes` instance

        """
        writer = io.BytesIO()
        writer.write(struct.pack('>HH', cls.CLASS_ID, cls.Methods.START_OK))
        encode_table(client_properties, writer)
        encode_short_string(auth_mechanism, writer)
        encode_long_string(auth_value, writer)
        encode_short_string(locale, writer)
        return writer.getvalue()

    @classmethod
    def construct_tune_ok(cls, channel_max, frame_max, heartbeat_delay):
        return struct.pack('>HHHIH',
                           cls.CLASS_ID, cls.Methods.TUNE_OK,
                           channel_max, frame_max, heartbeat_delay)

    @classmethod
    def construct_open(cls, virtual_host):
        return struct.pack('>HHBsBB',
                           cls.CLASS_ID, cls.Methods.OPEN,
                           len(virtual_host), virtual_host.encode('utf-8'),
                           0, 0)

    @classmethod
    def construct_secure_ok(cls, response):
        writer = io.BytesIO()
        writer.write(struct.pack('>HH', cls.CLASS_ID, cls.Methods.SECURE_OK))
        encode_long_string(response, writer)
        return writer.getvalue()

    def __str__(self):
        return '<Connection.{0}>'.format(self.Methods.name(self.method_id))


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
    if not frame_header:
        LOGGER.info('received zero bytes')
        return None

    frame_type, channel, frame_size = struct.unpack('>BHI', frame_header)
    frame_body = yield from reader.read(frame_size)
    frame_end = yield from reader.read(1)
    if frame_end != Frame.END_BYTE:
        raise errors.ProtocolFailure('invalid frame end ({0!r}) received',
                                     frame_end)

    frame = Frame(frame_type, channel, frame_body)
    if frame.frame_type == Frame.HEARTBEAT and frame.channel != 0:
        raise errors.ProtocolFailure('heartbeat received for channel {0}',
                                     channel)

    LOGGER.debug('decoded %s', frame)

    return frame


def write_frame(writer, frame_type, channel, frame_body):
    """
    Write an AMQP frame on `writer`.

    :param writer: instance to write frame to.  An instance of
        :class:`asyncio.StreamWriter` or :class:`io.BytesIO` will do
    :param int frame_type: type of frame to write
    :param int channel: channel that frame is associated with
    :param bytes frame_body: body of the frame
    :raise ValueError: if the frame type is not a valid frame type

    """
    if frame_type not in Frame.FRAME_TYPES:
        raise ValueError('Invalid frame type', frame_type)

    LOGGER.debug('writing frame type=%d channel=%d', frame_type, channel)
    writer.write(struct.pack('>BHI', frame_type, channel, len(frame_body)))
    writer.write(frame_body)
    writer.write(Frame.END_BYTE)
