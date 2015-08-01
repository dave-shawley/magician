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

    def __init__(self, user=None, password=None, virtual_host=None,
                 loop=None):
        self.logger = LOGGER.getChild('AMQPProtocol')
        self.loop = loop or asyncio.get_event_loop()
        super(AMQPProtocol, self).__init__(asyncio.StreamReader(),
                                           self.connected_to_server, loop)
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
            'receiver': None,
        }
        self._ecg = None
        self._closing = False

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

        self._ecg = _HeartMonitor(self.loop, frame.body.heartbeat_delay,
                                  self.close, self._send_heartbeat)
        self.writer.notify_write = self._ecg.data_sent

        self.logger.debug('issuing TuneOK channel_max=%d, frame_max=%d, '
                          'heartbeat_delay=%d', frame.body.channel_max,
                          frame.body.frame_max, self._ecg.frequency)
        frame_data = wire.Connection.construct_tune_ok(
            frame.body.channel_max, frame.body.frame_max, self._ecg.frequency)
        wire.write_frame(self.writer, wire.Frame.METHOD, 0, frame_data)

        self.logger.debug('connecting to virtual host %s', self.virtual_host)
        frame_data = wire.Connection.construct_open(self.virtual_host)
        wire.write_frame(self.writer, wire.Frame.METHOD, 0, frame_data)

        frame = yield from wire.read_frame(self.reader)
        yield from self._reject_unexpected_frame(
            frame, wire.Connection.CLASS_ID, wire.Connection.Methods.OPEN_OK)

        self.futures['connected'].set_result(True)

        future = self.loop.create_task(wire.read_frame(self.reader))
        future.add_done_callback(self._handle_unsolicited_frame)
        self.futures['receiver'] = future

    def close(self):
        if self.transport is None:
            return

        self.logger.info('closing connection')
        peer_close = self._closing
        self._closing = True

        if not self.futures['connected'].done():
            self.futures['connected'].set_exception(
                RuntimeError('closed before connected'))
            # sec 2.2.4 - There is no hand-shaking for errors on connections
            # that are not fully open. Following successful protocol header
            # negotiation, which is defined in detail later, and prior to
            # sending or receiving Open or Open-Ok, a peer that detects an
            # error MUST close the socket without sending any further data.
            self.transport.close()
            return

        if not peer_close:
            wire.write_frame(self.writer, wire.Frame.METHOD, 0,
                             wire.Connection.construct_close(200, 'OK'))
        else:
            self.transport.close()

    @property
    def is_active(self):
        """Is this instance active? (i.e, not closed)"""
        return (self.futures['connected'].done() and
                not self.futures['closed'].done())

    def connection_lost(self, exc):
        if not self._closing:
            self.logger.error('connection lost exception=%r', exc)
        else:
            self.logger.info('connection lost')

        if self._ecg:
            self._ecg.destroy()
            self._ecg = None

        if self.futures['receiver'] and not self.futures['receiver'].done():
            self.logger.debug('cancelling frame receiver')
            self.futures['receiver'].cancel()

        if not self.futures['closed'].done():
            if exc:
                self.futures['closed'].set_exception(exc)
            else:
                self.futures['closed'].set_result(True)

        self.reader = None
        self.writer = None
        self.transport = None

    @asyncio.coroutine
    def wait_closed(self):
        if self.transport is None:
            return

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

    def _handle_unsolicited_frame(self, future):
        self._ecg.heartbeat_received()
        if future.cancelled():
            self.logger.info('frame processing cancelled')
        elif future.exception():
            self.logger.error('exception when decoding '
                              'unsolicited frame - %r', future.exception())
        else:
            need_another_frame = True
            frame = future.result()
            if frame is None:
                if self.reader.at_eof():
                    self.logger.warning('EOF received while reading')
                    self.close()
                    return

                self.logger.warning('empty frame received')
            else:
                need_another_frame = self._process_frame(frame)

            if need_another_frame:
                self.logger.debug('scheduling next frame read')
                future = self.loop.create_task(wire.read_frame(self.reader))
                future.add_done_callback(self._handle_unsolicited_frame)
                self.futures['receiver'] = future

    def _process_frame(self, frame):
        """Process `frame` and decide whether we need to read or not."""
        self.logger.debug('processing %r', frame)
        if frame.frame_type == wire.Frame.METHOD:
            if frame.body.class_id == wire.Connection.CLASS_ID:
                if frame.body.method_id == wire.Connection.Methods.CLOSE:
                    if not self._closing:
                        self.logger.debug('sending close-ok')
                        wire.write_frame(self.writer, wire.Frame.METHOD,
                                         0, b'\x00\x0a\x00\x33')
                        self._closing = True
                        return False

                elif frame.body.method_id == wire.Connection.Methods.CLOSE_OK:
                    if self._closing:
                        self.logger.debug('received close-ok')
                        self.transport.close()
                        return False

                    self.logger.error('received unexpected close-ok')

        return True

    def _send_heartbeat(self):
        if self._closing:
            self.logger.debug('close in process, skipping heartbeat')
        else:
            self.logger.debug('sending heartbeat')
            wire.write_frame(self.writer, wire.Frame.HEARTBEAT, 0, b'')


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
    loop = loop or asyncio.get_event_loop()

    def create_protocol():
        return AMQPProtocol(parsed.username, parsed.password,
                            parsed.path, loop)

    transport, protocol = yield from loop.create_connection(
        create_protocol, host=parsed.hostname, port=parsed.port or 5672)

    yield from protocol.futures['connected']

    return protocol


class _HeartMonitor(object):
    """
    Monitors AMQP connection heartbeating

    :param asyncio.EventLoop loop: instance to schedule IO on
    :param int frequency: number of seconds between heartbeats
    :param close_cb: called with no parameters to close the
        connection.  This is used when two heartbeats are missed.
    :param send_heartbeat_cb: called with no parameters to issue
        a heartbeat on the connection

    """

    def __init__(self, loop, frequency, close_cb, send_heartbeat_cb):
        self._logger = LOGGER.getChild('HeartMonitor')
        self._close_connection = close_cb
        self._send_heartbeat = send_heartbeat_cb
        self._frequency = None
        self._handle = None

        self.last_send_time = loop.time()
        self.last_recv_time = loop.time()
        self.schedule = loop.call_at
        self.time = loop.time

        # set this LAST since it will schedule the next heartbeat
        self.frequency = frequency

    @property
    def frequency(self):
        """
        Number of seconds between sending heartbeats.

        Setting this value will adjust the heartbeat schedule immediately
        to ensure that heartbeats are not dropped.

        """
        return self._frequency

    @frequency.setter
    def frequency(self, value):
        self.cancel()
        self._frequency = value
        if value > 0:
            self._logger.debug('scheduling heartbeats every %fs', value)
            self._process()
        else:
            self._logger.debug('heartbeats are disabled')

    def _process(self):
        """
        Closes when we miss heartbeats and issues them if necessary.

        This method will reschedule itself and stores the the task handle
        in ``self._handle``.

        """
        self._handle = None
        now = self.time()
        if now > self.next_expected:
            self._logger.warn('have not received a heartbeat in %fs, closing',
                              now - self.last_recv_time)
            self._close_connection()
            return

        if now >= self.next_scheduled:
            self._send_heartbeat()
            self.last_send_time = now

        next_time = min(self.next_expected, self.next_scheduled)
        self._handle = self.schedule(next_time, self._process)

    def cancel(self):
        """Cancels any scheduled heartbeat call"""
        if self.scheduled:
            LOGGER.debug('cancelling heartbeat')
            self._handle.cancel()
            self._handle = None

    @property
    def enabled(self):
        """Are heartbeats enabled?"""
        return self.frequency > 0

    @property
    def next_scheduled(self):
        """Time of next scheduled heartbeat."""
        return self.last_send_time + self.frequency

    @property
    def next_expected(self):
        """Time of next expected heartbeat."""
        return self.last_recv_time + (2 * self.frequency)

    @property
    def scheduled(self):
        """Is there an outstanding heartbeat task?"""
        return self._handle is not None

    def heartbeat_received(self):
        """Reset the next expected heartbeat timer."""
        self.last_recv_time = self.time()

    def data_sent(self):
        """Reset the next expected heartbeat because we sent data."""
        self.last_send_time = self.time()

    def destroy(self):
        """
        Render the monitor inoperable.

        This method will cancel anything that is outstanding before
        destroying all internal state.  This is the best way to force
        the release of the callbacks which are potentially method
        handles.

        """
        self.cancel()

        # these reference IOLoop methods
        self.schedule = None
        self.time = None

        # these (probably) reference Protocol methods
        self._close_connection = None
        self._send_heartbeat = None
