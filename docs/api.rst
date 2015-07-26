Python API
==========

Standard Library Interface
--------------------------
.. py:currentmodule:: magician.amqp

The ``amqp`` module exposes the primary user interface of this library.
The :func:`.connect_to` co-routine is used to establish a new connection
to an AMQP broker.  When it returns, the connection is fully negotiated
and ready to be used.  See :ref:`implementation-details` for the gory
details.

.. autofunction:: connect_to

.. autoclass:: AMQPProtocol
   :members:

Off-the-wire Decoding Mechanisms
--------------------------------
.. automodule:: magician.wire
   :members:

Detected Errors
---------------
.. automodule:: magician.errors
   :members:

Utilities
---------
.. automodule:: magician.utils
   :members:
