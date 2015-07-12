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

from . import errors, wire


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
        self.transport = None
        self.futures = {
            'connected': asyncio.Future(),
            'closed': asyncio.Future(),
        }

    def connection_made(self, transport):
        self.transport = transport
        return super(AMQPProtocol, self).connection_made(transport)

    @asyncio.coroutine
    def connected_to_server(self, reader, writer):
        self.logger.debug('server connection established reader=%r, writer=%r',
                          reader, writer)
        self.reader = reader
        self.writer = writer

        self.writer.write(b'AMQP\x00\x00\x09\x01')
        frame = yield from wire.read_frame(self.reader)
        if frame.body.version_major != 0 or frame.body.version_minor != 9:
            yield from self._protocol_failure(
                'unsupported AMQP version {0}.{1}',
                frame.body.version_major, frame.body.version_minor)

        self.futures['connected'].set_result(True)

    def close(self):
        self.transport.close()

    def connection_lost(self, exc):
        self.logger.info('connection lost exception is %r', exc)
        if exc:
            self.futures['closed'].set_exception(exc)
        else:
            self.futures['closed'].set_result(True)

    @asyncio.coroutine
    def wait_closed(self):
        yield from self.futures['closed']

    def consume_from(self, queue_name, callback):
        pass

    @asyncio.coroutine
    def _protocol_failure(self, msg_format, *args):
        msg = msg_format.format(*args)
        self.logger.critical('%s', msg)
        self.close()
        yield from self.wait_closed()
        raise errors.ProtocolFailure('{0}', msg)


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
    :rtype: :class:`.AMQPProtocol`

    .. _AMQP URI Specification: https://www.rabbitmq.com/uri-spec.html

    """
    loop = loop or asyncio.get_event_loop()
    parsed = parse.urlsplit(amqp_url)
    transport, protocol = yield from loop.create_connection(
        AMQPProtocol, host=parsed.hostname, port=parsed.port or 5672)

    yield from protocol.futures['connected']

    return protocol
