from __future__ import print_function

import asyncio
import logging

from magician import amqp


def handle_message(connection, message):
    print('Received', message, 'from', connection)
    connection.ack(message)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG, datefmt='%H:%M:%S',
        format=('%(asctime)s.%(msecs)03d %(levelname)1.1s '
                '%(name)s: %(message)s'))
    loop = asyncio.get_event_loop()
    coro = amqp.connect_to('amqp://guest:guest@localhost:5672/')
    connection = loop.run_until_complete(coro)
    connection.consume_from('queue.name', handle_message)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    connection.close()
    loop.run_until_complete(connection.wait_closed())
    loop.close()
