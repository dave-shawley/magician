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
    def run_connected_to_server(self, reader, writer):
        proto = amqp.AMQPProtocol()
        proto.transport = helpers.FakeTransport(proto.connection_lost, None)
        reader.rewind()
        coro = proto.connected_to_server(reader, writer)
        asyncio.get_event_loop().run_until_complete(coro)

    def test_that_protocol_closes_on_server_version_mismatch(self):
        reader = helpers.AsyncBufferReader()
        reader.add_method_frame(wire.Connection.CLASS_ID,
                                wire.Connection.Methods.START, 0,
                                b'\x00\x0a', frames.CONNECTION_START[2:])
        with self.assertRaises(errors.ProtocolFailure):
            self.run_connected_to_server(reader, io.BytesIO())
