from urllib import parse
import asyncio
import io
import os
import struct
import unittest

import requests

from magician import amqp, errors, wire
from tests import frames, helpers


os.environ.setdefault('RABBITMQ_URL', 'amqp://localhost')


class ConnectToTests(unittest.TestCase):

    def test_that_protocol_closes_on_server_version_mismatch(self):
        frame_header = struct.pack('>BHI', wire.Frame.METHOD, 0,
                                   len(frames.CONNECTION_START))
        reader = helpers.AsyncBufferReader(
            frame_header
            + b'\x00\x0a\x00\x0a\x00\x0a'
            + frames.CONNECTION_START[6:]
            + b'\xCE')
        writer = io.BytesIO()
        protocol = amqp.AMQPProtocol()
        protocol.transport = helpers.FakeTransport(protocol.connection_lost,
                                                   None)
        loop = asyncio.get_event_loop()
        with self.assertRaises(errors.ProtocolFailure):
            loop.run_until_complete(protocol.connected_to_server(reader,
                                                                 writer))

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

    def test_that_protocol_connects_to_server(self):
        loop = asyncio.get_event_loop()
        conn = loop.run_until_complete(
            amqp.connect_to(os.environ['RABBITMQ_URL']))

        rabbit_info = parse.urlsplit(os.environ['RABBITMQ_URL'])
        sockinfo = conn.transport.get_extra_info('sockname')
        response = requests.get(
            'http://{0}:15672/api/connections'.format(rabbit_info.hostname),
            auth=(conn.user, conn.password))
        connections = [(conn['peer_host'], conn['peer_port'])
                       for conn in response.json()]
        self.assertIn((sockinfo[0], sockinfo[1]), connections)