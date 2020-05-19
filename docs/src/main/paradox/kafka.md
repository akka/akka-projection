# Messages from and to Kafka

A typical source for Projections is messages from Kafka. Akka Projections has integration with 
[Alpakka Kafka](https://doc.akka.io/docs/alpakka-kafka/current/), which is described in here.

The @apiddoc[KafkaSourceProvider] uses consumer group assignments from Kafka and can resume from offsets stored in
a database.

Akka Projections can store the offsets from Kafka in a @ref:[relational DB with Slick](slick.md).

The `SlickProjection` envelope handler returns a `DBIO` that will be run by the projection. This means that the target database
operations can be run in the same transaction as the storage of the offset, which means that @ref:[exactly-once](slick.md#exactly-once)
processing semantics is supported. It also offers @ref:[at-least-once](slick.md#at-least-once) semantics.

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
for messages from Kafka can be defined with the @apidoc[KafkaSourceProvider] like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #imports #sourceProvider }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

Please consult the [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html) for
specifics around the `ConsumerSettings`. The `KafkaSourceProvider` is using `Consumer.plainPartitionedManualOffsetSource`.

The `Projection` can then be defined as:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #exactlyOnce }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

and the `WordCountHandler`:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #handler }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

## Committing offset in Kafka

The `KafkaSourceProvider` described above stores the Kafka offsets in database. It is more common to
commit the offsets back to Kafka. The main advantage of storing the offsets in a database is that exactly-once
processing semantics can be achieved if the target database operations of the projection can be run in the same
transaction as the storage of the offset.

When using the approach of committing the offsets back to Kafka the [Alpakka Kafka comittableSource](https://doc.akka.io/docs/alpakka-kafka/current/consumer.html)
can be used, and Akka Projections is not needed for that usage.

## Sending to Kafka

The @apidoc[SendProducer] in Alpakka Kafka can be used for sending messages to Kafka from a Projection.

A `SlickHandler` that is sending to Kafka may look like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #wordPublisher }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

The `SendProducer` is constructed with:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #sendProducer }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

Please consult the [Alpakka Kafka documentation](https://doc.akka.io/docs/alpakka-kafka/current/producer.html) for
specifics around the `ProducerSettings` and `SendProducer`.

The `Projection` is defined as:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #sendToKafkaProjection }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }

where the `SourceProvider` in this example is:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #wordSource }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #todo }



