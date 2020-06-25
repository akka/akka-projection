# Processing with Akka Streams

An Akka Streams `FlowWithContext` can be used instead of a handler for processing the envelopes with at-least-once
semantics.

The following example is using the `CassandraProjection` but the flow would be the same if used
with `JdbcProjection` or `SlickProjection`.

Scala
:  @@snip [CassandraProjectionDocExample.scala](/examples/src/test/scala/docs/cassandra/CassandraProjectionDocExample.scala) { #atLeastOnceFlow }

Java
:  @@snip [CassandraProjectionDocExample.java](/examples/src/test/java/jdocs/cassandra/CassandraProjectionDocExample.java) { #atLeastOnceFlow }

The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried
in the context of the `FlowWithContext` and is stored in Cassandra when corresponding `Done` is emitted.
Since the offset is stored after processing the envelope, it means that if the projection is restarted
from previously stored offset some envelopes may be processed more than once.

There are a few caveats to be aware of:

* If the flow filters out envelopes the corresponding offset will not be stored, and such an envelope
  will be processed again if the projection is restarted and no later offset was stored.
* The flow should not duplicate emitted envelopes (`mapConcat`) with same offset, because then it can result in
  that the first offset is stored and when the projection is restarted that offset is considered completed even
  though more of the duplicated enveloped were never processed.
* The flow must not reorder elements, because the offsets may be stored in the wrong order
  and when the projection is restarted all envelopes up to the latest stored offset are considered
  completed even though some of them may not have been processed. This is the reason the flow is
  restricted to `FlowWithContext` rather than ordinary `Flow`.

