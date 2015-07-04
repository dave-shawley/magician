Magician
========
This library is the result of me learning the AMQP protocol in depth the
only way that I know how -- *by implementing a client of it*.  I also see
the need for a way to implement AMQP consumers and publishers without
dealing with every nuance of the AMQP specification.  Even better is to
develop an API from the outside in with how the API is used as the most
important aspect.  Many other implementations expose the AMQP details
directly and either expect the user to implement the entire AMQP consumer
stack by themselves or provide a thin veneer over the AMQP protocol.
I've found both approaches to be frustrating. *I just want to publish and
consume messages*.  That's how I got here and that is where this library
came from.

Dependencies
------------
* None currently, goal is for it to stay that way

Supported Versions
------------------
This library is being written from the ground up as a Python 3.4 project.
It is written directly over the `asyncio`_ standard library package.  It
can be ported using something like `trollius`_ if necessary.

.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _trollius: http://trollius.readthedocs.org
