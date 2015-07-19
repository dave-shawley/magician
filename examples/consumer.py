from __future__ import print_function

import asyncio
import logging
import sys

from magician import amqp


def handle_message(connection, message):
    print('Received', message, 'from', connection)
    connection.ack(message)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG, datefmt='%H:%M:%S',
        format=('%(asctime)s.%(msecs)03d %(levelname)1.1s '
                '%(name)s: %(message)s'))
    if len(sys.argv) > 1:
        amqp_url = sys.argv[1]
    else:
        amqp_url = 'amqp://guest:guest@localhost:5672/'

    loop = asyncio.get_event_loop()
    coro = amqp.connect_to(amqp_url)
    connection = loop.run_until_complete(coro)
    connection.consume_from('queue.name', handle_message)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    connection.close()
    loop.run_until_complete(connection.wait_closed())
    loop.close()
