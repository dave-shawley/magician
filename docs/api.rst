Python API
==========

Standard Library Interface
--------------------------
.. py:currentmodule:: magician.amqp

The ``amqp`` module exposes the primary user interface of this library.
The :func:`.connect_to` co-routine is used to establish a new connection
to an AMQP broker.  When it returns, the connection is fully negotiated
and ready to be used.  The following sequence diagram shows what is going
on behind the scenes.

.. seqdiag::

   seqdiag sequence {
      C [label="connect_to()"];
      A [label="loop"];
      P [label="protocol"];
      B [label="AMQP Broker"];
      C -> A [label="create_connection()"];
      A => P [label="<<create>>"];
      A ->> B [label="connect"];
      C <-- A [label=protocol];
      B ->> P [label="connection_made"];
      P -> B [label="Protocol Header"];
      B => P [label=Connection.Start, return=Connection.StartOK];
      B => P [label=Connection.Tune, return=Connection.TuneOK];
      P => B [label=Connection.Open, return=Connection.OpenOK];
      C <<- P [label="finish futures['connected']"];
   }

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
