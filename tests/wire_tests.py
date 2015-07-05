import io
import struct
import unittest

from magician import wire


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


class CanonicalFrameDecodingTests(unittest.TestCase):

    def test_that_connection_start_frame_decodes(self):
        view = memoryview(CONNECTION_START)
        table, offset = wire.decode_table(view, 6)
        self.assertEqual(offset, 429)
        self.assertEqual(table[b'product'], b'RabbitMQ')
        self.assertEqual(table[b'cluster_name'], b'rabbit@gondolin')
        self.assertEqual(table[b'copyright'],
                         b'Copyright (C) 2007-2014 GoPivotal, Inc.')
        self.assertEqual(table[b'version'], b'3.4.4')
        self.assertEqual(table[b'platform'], b'Erlang/OTP')
        self.assertEqual(table[b'information'],
                         b'Licensed under the MPL.  '
                         b'See http://www.rabbitmq.com/')
        self.assertEqual(table[b'capabilities'], {
            b'exchange_exchange_bindings': True,
            b'publisher_confirms': True,
            b'consumer_cancel_notify': True,
            b'authentication_failure_close': True,
            b'consumer_priorities': True,
            b'connection.blocked': True,
            b'basic.nack': True,
            b'per_consumer_qos': True,
        })


class FrameTests(unittest.TestCase):

    def test_that_connection_start_frame_decodes(self):
        server_properties, _ = wire.decode_table(CONNECTION_START, 6)

        frame = wire.Frame(1, 0, CONNECTION_START)
        self.assertEqual(frame.frame_type, wire.Frame.METHOD)
        self.assertEqual(frame.channel, 0)
        self.assertEqual(frame.raw_body, CONNECTION_START)
        self.assertEqual(frame.body.class_id, wire.Connection.CLASS_ID)
        self.assertEqual(frame.body.method_id, wire.Connection.Methods.START)
        self.assertEqual(frame.body.version_major, 0)
        self.assertEqual(frame.body.version_minor, 9)
        self.assertEqual(frame.body.server_properties, server_properties)
        self.assertEqual(frame.body.security_mechanisms,
                         [b'AMQPLAIN', b'PLAIN'])
        self.assertEqual(frame.body.locales, [b'en_US'])


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
