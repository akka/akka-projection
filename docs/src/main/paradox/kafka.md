# Messages from and to Kafka

A typical source for Projections is messages from Kafka. Akka Projections has integration with 
[Alpakka Kafka](https://doc.akka.io/docs/alpakka-kafka/current/), which is described in here.

The @apidoc[KafkaSourceProvider$] uses consumer group assignments from Kafka and can resume from offsets stored in
a database.

Akka Projections can store the offsets from Kafka in a @ref:[relational DB with JDBC](jdbc.md)
or in @ref:[relational DB with Slick](slick.md).

The `JdbcProjection` @scala[or `SlickProjection`] envelope handler will be run by the projection. This means that the target database
operations can be run in the same transaction as the storage of the offset, which means that @ref:[exactly-once](jdbc.md#exactly-once)
processing semantics is supported. It also offers @ref:[at-least-once](jdbc.md#at-least-once) semantics.

@@@ note

Offset storage of Kafka offsets are not implemented for Cassandra yet, see [issue #97](https://github.com/akka/akka-projection/issues/97).
 
@@@

A `Projection` can also @ref:[send messages to Kafka](#sending-to-kafka).

## Dependencies

To use the Kafka module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.lightbend.akka
  artifact=akka-projection-kafka_$scala.binary.version$
  version=$project.version$
}

Akka Projections require Akka $akka.version$ or later, see @ref:[Akka version](overview.md#akka-version).

@@project-info{ projectId="akka-projection-kafka" }

### Transitive dependencies

The table below shows `akka-projection-kafka`'s direct dependencies and the second tab shows all libraries it depends on transitively.

@@dependencies{ projectId="akka-projection-kafka" }

## KafkaSourceProvider

A @apidoc[SourceProvider] defines the source of the envelopes that the `Projection` will process. A `SourceProvider`
for messages from Kafka can be defined with the @apidoc[KafkaSourceProvider$] like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #imports #sourceProvider }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #imports #sourceProvider }

Please consult the [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html) for
specifics around the `ConsumerSettings`. The `KafkaSourceProvider` is using `Consumer.plainPartitionedManualOffsetSource`.

The `Projection` can then be defined as:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #exactlyOnce }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #exactlyOnce }

and the `WordCountHandler`:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #handler }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #handler }

## Committing offset in Kafka

The `KafkaSourceProvider` described above stores the Kafka offsets in a database. It is more common to
commit the offsets back to Kafka. The main advantage of storing the offsets in a database is that exactly-once
processing semantics can be achieved if the target database operations of the projection can be run in the same
transaction as the storage of the offset.

When using the approach of committing the offsets back to Kafka the [Alpakka Kafka comittableSource](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html)
can be used, and Akka Projections is not needed for that usage.

## Sending to Kafka

The @apidoc[SendProducer] in Alpakka Kafka can be used for sending messages to Kafka from a Projection.

A `JdbcHandler` that is sending to Kafka may look like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #wordPublisher }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #wordPublisher }

The `SendProducer` is constructed with:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #sendProducer }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #sendProducer }

Please consult the [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/producer.html) for
specifics around the `ProducerSettings` and `SendProducer`.

The `Projection` is defined as:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #sendToKafkaProjection }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #sendToKafkaProjection }

where the `SourceProvider` in this example is:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #wordSource }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #wordSource }

## Mergeable Offset

The offset type for a projection is determined by the @apidoc[SourceProvider] that's used.
Akka Projections supports a variety of offset types.
In most cases an event is associated with a single offset row in the projection implementation's offset store, but the @apidoc[KafkaSourceProvider$] uses a special type of offset called a @apidoc[MergeableOffset].

@apidoc[MergeableOffset] allows us to read and write a map of offsets to the projection offset store.
This is required because a subscription to consume from Kafka normally spans more than 1 Kafka Partition (see the [Apache Kafka documentation](https://kafka.apache.org/documentation/#intro_topics) for more information on Kafka's partitioning model).
To begin consuming from Kafka using offsets stored in a projection's offset store we must provide the Kafka Consumer with a map of topic partitions to offset map (a @scala[`java.util.Map[org.apache.kafka.common.TopicPartition, java.lang.Long]`]@java[`java.util.Map<org.apache.kafka.common.TopicPartition, java.lang.Long>`]).
The Kafka offset map is modelled as multiple rows in the projection offset table, where each row includes the projection name, a surrogate projection key that represents the Kafka topic partition, and the offset as a `java.lang.Long`.
When a projection with @apidoc[KafkaSourceProvider$] is started, or when a Kafka consumer group rebalance occurs, we read all the rows from the offset table for a projection name.
When an offset is committed we persist one or more rows of the Kafka offset map back to the projection offset table.

