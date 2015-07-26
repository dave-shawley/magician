.. _implementation-details:

AMQP Implementation Details
===========================
AMQP is a fairly complex protocol and this library tries very hard to make
it unnecessary for you to understand the details in order to use the library.
Since you might need to look inside of the box, this section describes some
of the details.  If you need more detail, the code is always there.  This
section should make things a little more readable though.

Connection Establishment
------------------------
The :func:`magician.amqp.connect_to` function initiates a connection with
an AMQP broker.  The connection negotiation happens asynchronously with the
function not returning until the connection is fully established.  The
following diagram shows what is going on behind the scenes:

.. seqdiag::

   seqdiag connect {
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

Connection Termination
----------------------
AMQP connections can be terminated from either side provided that a simple
protocol is obeyed.  The side initiating the close issues a "close" message.
When the peer receives this message, it issues a "close-ok" and closes the
underlying socket connection.  Note that the ordering of the socket closures
is unordered.
