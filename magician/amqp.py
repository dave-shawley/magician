"""
Functions for general use.

- :func:`.connect_to` initiates a connection to an AMQP broker

This module contains the *user-land* interface to interoperate
with an AMQP broker.

"""
from urllib import parse
import asyncio
import hashlib
import hmac
import logging
import sys

from . import errors, utils, wire, __version__


LOGGER = logging.getLogger(__name__)
DEFAULT_CLIENT_PROPERTIES = {
    'product': 'magician',
    'version': __version__,
    'platform': '{0} {1}'.format(
        sys.implementation.name,
        '.'.join(str(v) for v in sys.implementation.version[0:3])),
    'copyright': '2015 (c) Dave Shawley',
    'information': 'Quick, dirty, and native Python client',
    'capabilities': {},
}


class AMQPProtocol(asyncio.StreamReaderProtocol):
    """
    Asynchronously chat with a AMQP broker.

    :param str user: user to connect to the AMQP broker with
    :param str password: password to present to the AMQP broker

    You should not need to create instances of this class.  Use
    :func:`.connect_to` to connect to a AMQP broker and create
    a bound instance of this class.

    .. attribute:: futures

       A :class:`dict` containing :class:`asyncio.Future` instances
       that signal AMQP events.  The key is the name of the event:

       - **connected** completed when the AMQP connection is fully
         established.  See :meth:`.connected_to_server`.
       - **closed** completed when the AMQP connection is closed

    """

    def __init__(self, user=None, password=None, virtual_host=None):
        self.logger = LOGGER.getChild('AMQPProtocol')
        super(AMQPProtocol, self).__init__(asyncio.StreamReader(),
                                           self.connected_to_server)
        self.client_properties = DEFAULT_CLIENT_PROPERTIES.copy()
        self.user = user or 'guest'
        self.password = password or 'guest'
        self.virtual_host = virtual_host or '/'
        self.reader = None
        self.writer = None
        self.transport = None
        self.futures = {
            'connected': asyncio.Future(),
            'closed': asyncio.Future(),
        }

    def connection_made(self, transport):
        """Extended to save the transport for future use."""
        self.transport = transport
        return super(AMQPProtocol, self).connection_made(transport)

    @asyncio.coroutine
    def connected_to_server(self, reader, writer):
        """
        Handle the handshake with the AMQP broker.

        :param asyncio.StreamReader reader: reader side of the
            AMQP server connection
        :param asyncio.StreamWriter writer: writer side of the
            AMQP server connection

        This method is invoked when the server connection is established
        as part of the :class:`asyncio.StreamReaderProtocol`.  It is
        responsible for performing the AMQP connection negotiation and
        signals completion by finishing the ``self.futures['connected']``
        future.

        """
        self.logger.debug('server connection established transport=%r,'
                          'reader=%r, writer=%r', self.transport,
                          reader, writer)
        tracer = utils.IOTracer(reader, writer)
        self.reader = tracer
        self.writer = tracer

        self.writer.write(b'AMQP\x00\x00\x09\x01')
        frame = yield from wire.read_frame(self.reader)
        yield from self._reject_unexpected_frame(
            frame, wire.Connection.CLASS_ID, wire.Connection.Methods.START)
        if frame.body.version_major != 0 or frame.body.version_minor != 9:
            yield from self._protocol_failure(
                'unsupported AMQP version {0}.{1}',
                frame.body.version_major, frame.body.version_minor)

        frame = yield from self._authenticate(frame.body.security_mechanisms,
                                              frame.body.locales[0])

        self.logger.debug('issuing TuneOK channel_max=%d, frame_max=%d, '
                          'heartbeat_delay=%d', frame.body.channel_max,
                          frame.body.frame_max, frame.body.heartbeat_delay)
        frame_data = wire.Connection.construct_tune_ok(
            frame.body.channel_max, frame.body.frame_max,
            frame.body.heartbeat_delay)
        wire.write_frame(self.writer, wire.Frame.METHOD, 0, frame_data)

        self.logger.debug('connecting to virtual host %s', self.virtual_host)
        frame_data = wire.Connection.construct_open(self.virtual_host)
        wire.write_frame(self.writer, wire.Frame.METHOD, 0, frame_data)

        frame = yield from wire.read_frame(self.reader)
        yield from self._reject_unexpected_frame(frame, 10, 41)

        self.futures['connected'].set_result(True)

    def close(self):
        if not self.futures['connected'].done():
            self.futures['connected'].set_exception(
                RuntimeError('closed before connected'))
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
    def _authenticate(self, security_mechanisms, locale):
        self.logger.debug('authenticating as %s with password %s', self.user,
                          self.password[0] + '*****' + self.password[-1])
        if b'CRAM-MD5' in security_mechanisms:
            security_mechanism = 'CRAM-MD5'
            security_payload = ''
            expected = wire.Connection.Methods.SECURE
        else:
            security_mechanism = 'PLAIN'
            security_payload = '\x00{0}\x00{1}'.format(self.user,
                                                       self.password)
            expected = wire.Connection.Methods.TUNE

        self.logger.debug('selected %s from %s', security_mechanism,
                          security_mechanisms)
        wire.write_frame(
            self.writer, wire.Frame.METHOD, 0,
            wire.Connection.construct_start_ok(self.client_properties,
                                               security_mechanism,
                                               security_payload, locale))
        frame = yield from wire.read_frame(self.reader)
        yield from self._reject_unexpected_frame(frame,
                                                 wire.Connection.CLASS_ID,
                                                 expected)
        if expected == wire.Connection.Methods.SECURE:
            if security_mechanism == 'CRAM-MD5':
                challenge = frame.body.challenge
                digest = hmac.new(self.password.encode('utf-8'), msg=challenge,
                                  digestmod=hashlib.md5).hexdigest()
                response = self.user + ' ' + digest.lower()
                wire.write_frame(self.writer, wire.Frame.METHOD, 0,
                                 wire.Connection.construct_secure_ok(response))
                frame = yield from wire.read_frame(self.reader)
                yield from self._reject_unexpected_frame(
                    frame, wire.Connection.CLASS_ID,
                    wire.Connection.Methods.TUNE)
            else:
                raise RuntimeError('unhandled security mechanism ' +
                                   security_mechanism)

        return frame

    @asyncio.coroutine
    def _protocol_failure(self, msg_format, *args):
        msg = msg_format.format(*args)
        self.logger.critical('%s', msg)
        self.close()
        yield from self.wait_closed()
        raise errors.ProtocolFailure('{0}', msg)

    @asyncio.coroutine
    def _reject_unexpected_frame(self, frame, expected_class, expected_method):
        if (frame.body.class_id != expected_class or
                frame.body.method_id != expected_method):
            yield from self._protocol_failure(
                'expected frame ({0}, {1}), received ({2}, {3})',
                expected_class, expected_method,
                frame.body.class_id, frame.body.method_id)


@asyncio.coroutine
def connect_to(amqp_url, loop=None):
    """
    Open a connection to an AMQP broker.

    :param str amqp_url: connection information formatted as described
        in the `AMQP URI Specification`_
    :param asyncio.BaseEventLoop loop: optional event loop to connect
        to the broker with.  If unspecified, the value returned from
        :func:`asyncio.get_event_loop` is used.
    :return: connected protocol instance
    :rtype: :class:`.AMQPProtocol`

    .. _AMQP URI Specification: https://www.rabbitmq.com/uri-spec.html

    """
    parsed = parse.urlsplit(amqp_url)

    def create_protocol():
        return AMQPProtocol(parsed.username, parsed.password, parsed.path)

    loop = loop or asyncio.get_event_loop()
    transport, protocol = yield from loop.create_connection(
        create_protocol, host=parsed.hostname, port=parsed.port or 5672)

    yield from protocol.futures['connected']

    return protocol
