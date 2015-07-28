"""
Utilities to ease development and debugging.

- :func:`.hexdump` writes a canonical hexadecimal dump of bytes
  to a logger at "debug" level
- :class:`.IOTracer` wraps asyncio streaming classes and uses
  :func:`.hexdump` to dump them to a log

This module contains utility classes and functions that make
development and debugging just a little easier.  They are not
meant for general purpose use, but they are publicly available
and documented so *caveat emptor*.

"""
import asyncio
import logging


_CHECKS = tuple(zip([24, 16, 8, 0], [0xFFFFFF, 0xFFFF, 0xFF, 0]))


def hexdump(logger, buffer):
    """
    Hex dump a buffer to a logger.

    :param logging.Logger logger: the logger to dump bytes to
    :param buffer: the buffer to dump

    This function uses a hexdump format inspired by the various
    BSD hexdump programs.  Each line represents up to 16 bytes
    in two-character hexadecimal.  The line starts with the
    offset as a eight-character hexadecimal value, followed by
    sixteen one-byte values, and the ASCII representation of
    the byte stream::

        00000000 | 64 65 66 20 68 65 78 64  ... 6f 67 67 | def hexdump(logg |
        00000010 | 65 72 2c 20 62 75 66 66  ... 20 20 20 | er, buffer):.    |
        00000020 | 20 69 66 20 6e 6f 74 20  ... 72 2e 69 |  if not logger.i |

    Each output segment is exactly 80 characters in width and
    non-ASCII characters are represented as ``.`` in the ASCII
    output.

    """
    if not logger.isEnabledFor(logging.DEBUG):
        return

    try:
        view = memoryview(buffer)
    except TypeError:
        raw_bytes = bytearray()
        for value in (ord(ch) for ch in buffer):
            capture = False
            for shift, check in _CHECKS:
                capture = capture or value > check
                if capture:
                    raw_bytes.append((value >> shift) & 0xFF)
        view = memoryview(raw_bytes)

    for offset in range(0, len(view), 16):
        line = view[offset:offset+16]
        first_octobyte = ' '.join('{0:02x}'.format(b) for b in line[:8])
        next_octobyte = ' '.join('{0:02x}'.format(b) for b in line[8:])
        printable = ''.join(chr(b) if 0x20 <= b <= 0x7e else '.'
                            for b in line)
        logger.debug('%08x | %-23.23s  %-23.23s | %-16.16s |',
                     offset, first_octobyte, next_octobyte, printable)


class IOTracer(object):
    """
    Wraps asyncio StreamReader/Writer and dumps bytes.

    :param asyncio.StreamReader reader: the reader to wrap
    :param asyncio.StreamWriter writer: the writer to wrap

    This little class wraps a :class:`asyncio.StreamReader` and
    :class:`asyncio.StreamWriter` pair and dumps the byte stream
    to a logger in the canonical hex-dump format.

    .. attribute:: logger

       :class:`logging.Logger` instance that bytes will be dumped on.

    .. attribute:: notify_write

       If this is not :data:`None`, then it will be called whenever
       data is written.  This is most useful to update the heartbeat
       timer when data is sent.

    """
    logger = logging.getLogger('magician.trace')

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.notify_write = None

    @asyncio.coroutine
    def read(self, num_bytes):
        """
        Async read using :meth:`asyncio.StreamReader.read`.

        :param int num_bytes: number of bytes to read
        :return: bytes read

        Also *hexdumps* the buffer to :attr:`.logger` before returning.

        """
        self.logger.debug('reading %d bytes', num_bytes)
        buffer = yield from self.reader.read(num_bytes)
        hexdump(self.logger, buffer)
        return buffer

    def at_eof(self):
        """Return self.reader.at_eof()"""
        return self.reader.at_eof()

    def write(self, buffer):
        """
        Write `buffer` using :meth:`asyncio.StreamWriter.write`.

        :param buffer: buffer to write

        Also *hexdumps* the buffer to :attr:`self.logger`.

        """
        self.logger.debug('writing %d bytes', len(buffer))
        hexdump(self.logger, buffer)
        self.writer.write(buffer)
        if self.notify_write:
            self.notify_write()
