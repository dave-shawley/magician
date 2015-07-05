"""
Functions for general use.

- :func:`.connect_to` initiates a connection to an AMQP broker

This module contains the *user-land* interface to interoperate
with an AMQP broker.

"""
import asyncio
import logging
try:
    from urllib import parse
except ImportError:
    import urlparse as parse

from . import wire


LOGGER = logging.getLogger(__name__)


class AMQPProtocol(asyncio.StreamReaderProtocol):
    """
    Asynchronously chat with a AMQP broker.
    """

    def __init__(self):
        self.logger = LOGGER.getChild('AMQPProtocol')
        super(AMQPProtocol, self).__init__(asyncio.StreamReader(),
                                           self.connected_to_server)
        self.reader = None
        self.writer = None

    @asyncio.coroutine
    def connected_to_server(self, reader, writer):
        self.logger.debug('server connection established reader=%r, writer=%r',
                          reader, writer)
        self.reader = reader
        self.writer = writer

        self.writer.write(b'AMQP\x00\x00\x09\x01')
        frame = yield from wire.read_frame(self.reader)

    def close(self):
        pass

    @asyncio.coroutine
    def wait_closed(self):
        pass

    def consume_from(self, queue_name, callback):
        pass


@asyncio.coroutine
def connect_to(amqp_url, loop=None):
    """
    Open a connection to an AMQP broker.

    :param str amqp_url: connection information formatted as described
        in the `AMQP URI Specification`_
    :param asyncio.BaseEventLoop loop: optional event loop to connect
        to the broker with.  If unspecified, the value returned from
        :func:`asyncio.get_event_loop` is used.
    :return: instance of :class:`.AMQPProtocol`

    .. _AMQP URI Specification: https://www.rabbitmq.com/uri-spec.html

    """
    loop = loop or asyncio.get_event_loop()
    parsed = parse.urlsplit(amqp_url)
    transport, protocol = yield from loop.create_connection(
        AMQPProtocol, host=parsed.hostname, port=parsed.port or 5672)

    return protocol
