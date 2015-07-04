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
