import asyncio
import io
import struct
import unittest

from magician import errors, wire


CONNECTION_START = (
    b'\x00\x0a'
    b'\x00\x0a'
    b'\x00'
    b'\x09'
    b'\x00\x00\x01\xa3'
    b'\x0CcapabilitiesF\x00\x00\x00\xb5'
    b'\x12publisher_confirmst\x01'
    b'\x1Aexchange_exchange_bindingst\x01'
    b'\x0abasic.nackt\x01'
    b'\x16consumer_cancel_notifyt\x01'
    b'\x12connection.blockedt\x01'
    b'\x13consumer_prioritiest\x01'
    b'\x1Cauthentication_failure_closet\x01'
    b'\x10per_consumer_qost\x01'
    b'\x0Ccluster_nameS\x00\x00\x00\x0frabbit@gondolin'
    b'\x09copyrightS\x00\x00\x00\x27Copyright (C) 2007-2014 GoPivotal, Inc.'
    b'\x0BinformationS\x00\x00\x00\x35Licensed under the MPL.  See '
    b'http://www.rabbitmq.com/'
    b'\x08platformS\x00\x00\x00\x0AErlang/OTP'
    b'\x07productS\x00\x00\x00\x08RabbitMQ'
    b'\x07versionS\x00\x00\x00\x053.4.4'
    b'\x00\x00\x00\x0eAMQPLAIN PLAIN'
    b'\x00\x00\x00\x05en_US')

TUNE = (
    b'\x00\x0a'
    b'\x00\x1e'
    b'\x00\x00'
    b'\x00\x02\x00\x00'
    b'\x02\x44'
)


class FrameTests(unittest.TestCase):

    def setUp(self):
        super(FrameTests, self).setUp()
        self.loop = asyncio.get_event_loop()

    def process_frame(self, frame_type, channel, frame_body):
        buffer = io.BytesIO()
        buffer.write(struct.pack('>BHI', frame_type, channel, len(frame_body)))
        buffer.write(frame_body)
        buffer.write(wire.Frame.END_BYTE)
        reader = AsyncBufferReader(buffer.getvalue())
        return self.loop.run_until_complete(wire.read_frame(reader))

    def test_that_connection_start_frame_decodes(self):
        expected_properties, _ = wire.decode_table(CONNECTION_START, 6)
        frame = self.process_frame(wire.Frame.METHOD, 0, CONNECTION_START)

        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.channel, 0)
        self.assertEqual(frame.raw_body, CONNECTION_START)
        self.assertEqual(frame.body.class_id, wire.Connection.CLASS_ID)
        self.assertEqual(frame.body.method_id, wire.Connection.Methods.START)
        self.assertEqual(frame.body.version_major, 0)
        self.assertEqual(frame.body.version_minor, 9)
        self.assertEqual(frame.body.server_properties, expected_properties)
        self.assertEqual(frame.body.security_mechanisms,
                         [b'AMQPLAIN', b'PLAIN'])
        self.assertEqual(frame.body.locales, [b'en_US'])

    def test_that_connection_tune_decodes(self):
        frame = self.process_frame(wire.Frame.METHOD, 0, TUNE)

        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.channel, 0)
        self.assertEqual(frame.raw_body, TUNE)
        self.assertEqual(frame.body.channel_max, 0)
        self.assertEqual(frame.body.frame_max, 0x20000)
        self.assertEqual(frame.body.heartbeat_delay, 0x244)

    def test_that_missing_frame_end_raises_exception(self):
        buffer = io.BytesIO()
        buffer.write(struct.pack('>BHI', wire.Frame.METHOD, 0, len(TUNE)))
        buffer.write(TUNE)
        # omitting this: buffer.write(wire.Frame.END_BYTE)

        reader = AsyncBufferReader(buffer.getvalue())
        with self.assertRaises(errors.ProtocolFailure):
            self.loop.run_until_complete(wire.read_frame(reader))

    def test_that_heartbeat_on_specific_channel_raises_exception(self):
        with self.assertRaises(errors.ProtocolFailure):
            self.process_frame(wire.Frame.HEARTBEAT, 1, b'')


class EncodingTests(unittest.TestCase):
    UNICODE_VALUE = u'Ang\u00E9liqu\u00E9'
    UTF8_VALUE = UNICODE_VALUE.encode('utf-8')

    def setUp(self):
        super(EncodingTests, self).setUp()
        self.writer = io.BytesIO()

    @property
    def written(self):
        return self.writer.getvalue()

    def test_that_encode_short_string_writes_bytes_as_is(self):
        wire.encode_short_string(self.UTF8_VALUE, self.writer)
        self.assertEqual(self.written[0], len(self.UTF8_VALUE))
        self.assertEqual(self.written[1:], self.UTF8_VALUE)

    def test_that_encode_short_string_utf8_encodes_string(self):
        wire.encode_short_string(self.UNICODE_VALUE, self.writer)
        self.assertEqual(self.written[0], len(self.UTF8_VALUE))
        self.assertEqual(self.written[1:], self.UTF8_VALUE)

    def test_that_encode_short_string_rejects_long_strings(self):
        with self.assertRaises(ValueError):
            wire.encode_short_string(bytes(256), self.writer)

    def test_that_encode_long_string_writes_bytes_as_is(self):
        wire.encode_long_string(self.UTF8_VALUE, self.writer)
        self.assertEqual(struct.unpack('>I', self.written[0:4])[0],
                         len(self.UTF8_VALUE))
        self.assertEqual(self.written[4:], self.UTF8_VALUE)

    def test_that_encode_long_string_utf8_encodes_string(self):
        wire.encode_long_string(self.UNICODE_VALUE, self.writer)
        self.assertEqual(struct.unpack('>I', self.written[0:4])[0],
                         len(self.UTF8_VALUE))
        self.assertEqual(self.written[4:], self.UTF8_VALUE)

    def test_that_encode_table_handles_expected_value_types(self):
        wire.encode_table({
            'bytes_value': b'encoded as long string',
            'dict': {'encoded': 'as table'},
            'empty_dict': {},
            'false_value': False,
            'string_value': 'encoded as long string',
            'true_value': True,
        }, self.writer)
        self.assertEqual(
            self.written,
            b'\x00\x00\x00\x99'
            b'\x0Bbytes_valueS\x00\x00\x00\x16encoded as long string'
            b'\x04dictF\x00\x00\x00\x15\x07encodedS\x00\x00\x00\x08as table'
            b'\x0Aempty_dictF\x00\x00\x00\x00'
            b'\x0Bfalse_valuet\x00'
            b'\x0Cstring_valueS\x00\x00\x00\x16encoded as long string'
            b'\x0Atrue_valuet\x01'
        )

    def test_that_encode_table_raises_exception_on_unhandled_types(self):
        with self.assertRaises(ValueError):
            wire.encode_table({'object_value': object()}, self.writer)

    def test_that_write_frame_correctly_encodes_data(self):
        wire.write_frame(self.writer, 1, 0x1234, b'frame-body-is-opaque')
        self.assertEqual(self.written[0], 1)
        self.assertEqual(self.written[1:3], b'\x12\x34')
        self.assertEqual(self.written[3:7], b'\x00\x00\x00\x14')  # 20 bytes
        self.assertEqual(self.written[7:-1], b'frame-body-is-opaque')
        self.assertEqual(self.written[-1], 0xCE)

    def test_that_write_frame_rejects_invalid_frame_types(self):
        with self.assertRaises(ValueError):
            wire.write_frame(self.writer, 10, 1, b'')

    def test_that_construct_start_ok_returns_encoded_value(self):
        encoded = wire.Connection.construct_start_ok(
            {'client': 'properties'},
            'auth mechanism', '\x00name\x00password',
            'locale',
        )
        self.assertEqual(
            encoded,
            b'\x00\x0A'
            b'\x00\x0B'
            b'\x00\x00\x00\x16'
            b'\x06clientS\x00\x00\x00\x0Aproperties'
            b'\x0Eauth mechanism'
            b'\x00\x00\x00\x0E\x00name\x00password'
            b'\x06locale'
        )


class AsyncBufferReader(object):
    """Simple implementation of asyncio.StreamReader over a buffer."""

    def __init__(self, buffer):
        super(AsyncBufferReader, self).__init__()
        self.buffer = buffer
        self.stream = io.BytesIO(self.buffer)

    @asyncio.coroutine
    def read(self, num_bytes):
        return self.stream.read(num_bytes)
