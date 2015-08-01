import asyncio
import io
import struct
import unittest

from magician import errors, wire
from tests import frames, helpers


class FrameTests(unittest.TestCase):

    def setUp(self):
        super(FrameTests, self).setUp()
        self.loop = asyncio.get_event_loop()

    def process_method_frame(self, class_id, method_id, frame_body):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(class_id, method_id, 0, frame_body)
        return self.loop.run_until_complete(wire.read_frame(reader))

    def test_that_connection_start_frame_decodes(self):
        expected_properties, _ = wire.decode_table(frames.CONNECTION_START, 2)
        frame = self.process_method_frame(wire.Connection.CLASS_ID,
                                          wire.Connection.Methods.START,
                                          frames.CONNECTION_START)

        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.channel, 0)
        self.assertEqual(frame.body.class_id, wire.Connection.CLASS_ID)
        self.assertEqual(frame.body.method_id, wire.Connection.Methods.START)
        self.assertEqual(frame.body.version_major, 0)
        self.assertEqual(frame.body.version_minor, 9)
        self.assertEqual(frame.body.server_properties, expected_properties)
        self.assertEqual(frame.body.security_mechanisms,
                         [b'AMQPLAIN', b'PLAIN'])
        self.assertEqual(frame.body.locales, [b'en_US'])

    def test_that_connection_tune_decodes(self):
        frame = self.process_method_frame(wire.Connection.CLASS_ID,
                                          wire.Connection.Methods.TUNE,
                                          frames.TUNE)

        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.channel, 0)
        self.assertEqual(frame.body.channel_max, 0)
        self.assertEqual(frame.body.frame_max, 0x20000)
        self.assertEqual(frame.body.heartbeat_delay, 0x244)

    def test_that_missing_frame_end_raises_exception(self):
        # build a frame that omits the end byte
        frame = io.BytesIO()
        frame.write(struct.pack('>BHI', wire.Frame.METHOD, 0,
                                len(frames.TUNE)))

        reader = helpers.AsyncBufferReader(emulate_eof=True)
        reader.emit_frame(frame.getvalue())
        with self.assertRaises(errors.ProtocolFailure):
            self.loop.run_until_complete(wire.read_frame(reader))

    def test_that_heartbeat_on_specific_channel_raises_exception(self):
        reader = helpers.AsyncBufferReader()
        reader.add_frame(wire.Frame.HEARTBEAT, 23, b'')
        with self.assertRaises(errors.ProtocolFailure):
            self.loop.run_until_complete(wire.read_frame(reader))

    def test_that_partial_reads_are_supported(self):
        frame = frames.CONNECTION_START[:]
        reader = asyncio.StreamReader()
        reader.feed_data(struct.pack('>BHIHH',
                                     wire.Frame.METHOD, 0,
                                     len(frame) + 4,
                                     wire.Connection.CLASS_ID,
                                     wire.Connection.Methods.START))

        task = self.loop.create_task(wire.read_frame(reader))
        while frame:
            reader.feed_data(frame[:42])
            frame = frame[42:]
            self.loop.run_until_complete(asyncio.wait([task], timeout=0.01))
        reader.feed_data(wire.Frame.END_BYTE)
        frame = self.loop.run_until_complete(task)
        self.assertIsNotNone(frame)
        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.body.class_id, wire.Connection.CLASS_ID)
        self.assertEqual(frame.body.method_id, wire.Connection.Methods.START)


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
