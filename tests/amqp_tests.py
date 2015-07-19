from urllib import parse
import asyncio
import io
import logging
import os
import socket
import unittest

import requests

from magician import amqp, errors, wire
from tests import frames, helpers


os.environ.setdefault('RABBITMQ_URL', 'amqp://localhost')
_rabbitmq_available = None


def rabbitmq_available():
    global _rabbitmq_available

    if _rabbitmq_available is not None:
        return _rabbitmq_available

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM,
                         socket.IPPROTO_TCP)
    sock.settimeout(0.25)
    try:
        sock.connect(('127.0.0.1', 5672))
        _rabbitmq_available = True
    except:
        logging.getLogger(__name__).info('AMQP server is not available')
        _rabbitmq_available = False
    return _rabbitmq_available


class ConnectToTests(unittest.TestCase):

    def test_that_connect_to_passes_credentials_to_protocol(self):
        _, protocol = yield amqp.connect_to(
            'amqp://user%2dname:p%40ss%3aword@rabbit.host.name:36813/%2Fdev',
            loop=helpers.FakeEventLoop())
        self.assertEqual(protocol.user, 'user-name')
        self.assertEqual(protocol.password, 'p@ss:word')

    def test_that_connect_to_passes_virtual_host_to_protocol(self):
        _, protocol = yield amqp.connect_to(
            'amqp://user%2dname:p%40ss%3aword@rabbit.host.name:36813/%2Fdev',
            loop=helpers.FakeEventLoop())
        self.assertEqual(protocol.virtual_host, '/dev')

    @unittest.skipUnless(rabbitmq_available(), 'RabbitMQ server not available')
    def test_that_protocol_connects_to_server(self):
        loop = asyncio.get_event_loop()
        conn = loop.run_until_complete(
            amqp.connect_to(os.environ['RABBITMQ_URL']))

        rabbit_info = parse.urlsplit(os.environ['RABBITMQ_URL'])
        sockinfo = conn.transport.get_extra_info('sockname')
        response = requests.get(
            'http://{0}:15672/api/connections'.format(rabbit_info.hostname),
            auth=(conn.user, conn.password))
        connections = [(conn_info['peer_host'], conn_info['peer_port'])
                       for conn_info in response.json()]
        self.assertIn((sockinfo[0], sockinfo[1]), connections)


class ProtocolViolationTests(unittest.TestCase):

    def setUp(self):
        super(ProtocolViolationTests, self).setUp()
        self.protocol = amqp.AMQPProtocol()
        self.transport = helpers.FakeTransport(
            self.protocol.connection_lost, None)
        self.protocol.transport = self.transport

    def run_connected_to_server(self, reader, writer):
        reader.rewind()
        coro = self.protocol.connected_to_server(reader, writer)
        asyncio.get_event_loop().run_until_complete(coro)

    def assert_protocol_is_closed(self):
        self.assertIs(self.transport.closed, True)
        self.assertTrue(self.protocol.futures['connected'].done())
        self.assertTrue(self.protocol.futures['closed'].done())

    def test_that_protocol_closes_on_server_version_mismatch(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                b'\x00\x0a', frames.CONNECTION_START[2:])
        with self.assertRaises(errors.ProtocolFailure):
            self.run_connected_to_server(reader, io.BytesIO())
        self.assert_protocol_is_closed()

    def test_that_protocol_closes_on_missing_start_frame(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.TUNE, 0,
                                frames.TUNE)
        with self.assertRaises(errors.ProtocolFailure):
            self.run_connected_to_server(reader, io.BytesIO())
        self.assert_protocol_is_closed()

    def test_that_protocol_closes_on_missing_tune_frame(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                frames.CONNECTION_START)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                frames.CONNECTION_START)
        with self.assertRaises(errors.ProtocolFailure):
            self.run_connected_to_server(reader, io.BytesIO())
        self.assert_protocol_is_closed()

    def test_that_protocol_closes_on_missing_open_ok_frame(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                frames.CONNECTION_START)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.TUNE, 0,
                                frames.TUNE)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.TUNE, 0,
                                frames.TUNE)
        with self.assertRaises(errors.ProtocolFailure):
            self.run_connected_to_server(reader, io.BytesIO())
        self.assert_protocol_is_closed()


class ProtocolAuthenticationTests(unittest.TestCase):

    def setUp(self):
        super(ProtocolAuthenticationTests, self).setUp()
        self.protocol = amqp.AMQPProtocol()
        self.transport = helpers.FakeTransport(
            self.protocol.connection_lost, None)
        self.protocol.transport = self.transport

    def run_connected_to_server(self, reader, writer):
        reader.rewind()
        coro = self.protocol.connected_to_server(reader, writer)
        asyncio.get_event_loop().run_until_complete(coro)

    def test_that_default_auth_is_plain(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                frames.CONNECTION_START)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.TUNE, 0,
                                frames.TUNE)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.OPEN_OK, 0,
                                frames.OPEN_OK)

        writer = helpers.FrameReceiver()
        self.run_connected_to_server(reader, writer)
        self.assertEqual(writer.frames[0]['body'].method_id,
                         wire.Connection.Methods.START_OK)

        data = writer.frames[0]['body'].method_body
        _, offset = wire.decode_table(data, 0)
        auth_mechanism, offset = wire.decode_short_string(data, offset)
        auth_value, _ = wire.decode_long_string(data, offset)
        self.assertEqual(auth_mechanism, b'PLAIN')
        self.assertEqual(auth_value, b'\x00guest\x00guest')

    def test_that_scram_md5_is_used_if_available(self):
        frame_data = bytearray(frames.CONNECTION_START)
        frame_data[-27:] = (b'\x00\x00\x00\x0ePLAIN CRAM-MD5' +
                            b'\x00\x00\x00\x05en_US')
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                frame_data)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.SECURE, 0,
                                frames.SECURE)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.TUNE, 0,
                                frames.TUNE)
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.OPEN_OK, 0,
                                frames.OPEN_OK)

        writer = helpers.FrameReceiver()
        self.run_connected_to_server(reader, writer)
        self.assertEqual(writer.frames[0]['body'].method_id,
                         wire.Connection.Methods.START_OK)
        data = writer.frames[0]['body'].method_body
        _, offset = wire.decode_table(data, 0)
        auth_mechanism, offset = wire.decode_short_string(data, offset)
        auth_value, _ = wire.decode_long_string(data, offset)
        self.assertEqual(auth_mechanism, b'CRAM-MD5')
        self.assertEqual(auth_value, b'')

        self.assertEqual(writer.frames[1]['body'].method_id,
                         wire.Connection.Methods.SECURE_OK)
        data = writer.frames[1]['body'].method_body
        response, _ = wire.decode_long_string(data, 0)
        self.assertEqual(response, b'guest cd9f33372ef70fdd8fe495fe61446cf2')
