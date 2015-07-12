import asyncio
import io
import os
import struct
import unittest

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
