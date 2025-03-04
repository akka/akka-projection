# Messages from and to Kafka

A typical source for Projections is messages from Kafka. Akka Projections supports integration with Kafka using @extref:[Alpakka Kafka](alpakka-kafka:).

The @apidoc[KafkaSourceProvider$] uses consumer group assignments from Kafka and can resume from offsets stored in a database.

Akka Projections can store the offsets from Kafka in a @ref:[relational DB with JDBC](jdbc.md)
or in @ref:[relational DB with Slick](slick.md).

The `JdbcProjection` @scala[or `SlickProjection`] envelope handler will be run by the projection. This means that the target database operations can be run in the same transaction as the storage of the offset, which means when used with @ref:[exactly-once](jdbc.md#exactly-once) the offsets will be persisted on the same transaction as the projected model (see @ref:[Committing offset outside Kafka](#committing-offset-outside-kafka)). It also offers @ref:[at-least-once](jdbc.md#at-least-once) semantics.

@@@ note

Offset storage of Kafka offsets are not implemented for Cassandra yet, see [issue #97](https://github.com/akka/akka-projection/issues/97).

@@@

A `Projection` can also @ref:[send messages to Kafka](#sending-to-kafka).

## Dependencies

The Akka dependencies are available from Akka's library repository. To access them there, you need to configure the URL for this repository.

@@repository [sbt,Maven,Gradle] {
id="akka-repository"
name="Akka library repository"
url="https://repo.akka.io/maven"
}

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

@@@ warning { title=Important }
Due to the mutable state inside @apidoc[KafkaSourceProvider$], DO NOT share the instance of provider across projections.

For example, if you distribute projection via @apidoc[ShardedDaemonProcess], instantiate each provider inside the behavior
factory.
@@@

A @apidoc[SourceProvider] defines the source of the envelopes that the `Projection` will process. A `SourceProvider`
for messages from Kafka can be defined with the @apidoc[KafkaSourceProvider$] like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #imports #sourceProvider }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #imports #sourceProvider }

Please consult the @extref:[Alpakka Kafka documentation](alpakka-kafka:consumer.html) for
specifics around the `ConsumerSettings`. The `KafkaSourceProvider` is using `Consumer.plainPartitionedManualOffsetSource`.

The `Projection` can then be defined as:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #exactlyOnce }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #exactlyOnce }

and the `WordCountJdbcHandler`:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #exactly-once-jdbc-handler }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #exactly-once-jdbc-handler }

Where the `WordRepository` is an implementation of:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #repository }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #repository }

## Committing offset outside Kafka

The `KafkaSourceProvider` described above stores the Kafka offsets in a database. The main advantage of storing the offsets in a database is that exactly-once processing semantics can be achieved if the target database operations of the projection can be run in the same transaction as the storage of the offset.

However, there is a caveat when choosing for `exactly-once`. When the Kafka Consumer Group rebalance occurs it's possible that some messages from a revoked partitions are still in-flight and have not yet been committed to the offset store. Projections will attempt to filter out such messages, but it's not possible to guarantee it all the time.

To mitigate that risk, you can increase the value of `akka.projection.kafka.read-offset-delay` (defaults to 500ms). This delay adds a buffer of time between when the Kafka Source Provider starts up, or when it's assigned a new partition, to retrieve the map of partitions to offsets to give any projections running in parallel a chance to drain in-flight messages.

## Committing offset in Kafka

When using the approach of committing the offsets back to Kafka the @extref:[Alpakka Kafka comittableSource](alpakka-kafka:consumer.html) can be used, and Akka Projections is not needed for that usage.

## Sending to Kafka

To send events to Kafka one can use @apidoc[SendProducer] or @apidoc[Producer.flowWithContext](Producer$) method in Alpakka Kafka.

### Sending to Kafka using the SendProducer

An async `Handler` that is sending to Kafka may look like this:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #wordPublisher }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #wordPublisher }

The `SendProducer` is constructed with:

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #imports-producer #sendProducer }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #imports-producer #sendProducer }

Please consult the @extrefL[Alpakka Kafka documentation](alpakka-kafka:producer.html) for
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

### Sending to Kafka using a Producer Flow

Alternatively, we can define the same projection using @apidoc[Producer.flowWithContext](Producer$) in combination with `atLeastOnceFlow`.

The `WordSource` emits `WordEnvelope`s, therefore we will build a flow that takes every single emitted `WordEnvelope` and map it into an Alpakka Kafka @apidoc[ProducerMessage$]. The `ProducerMessage` factory methods can be used to produce a single message, multiple messages, or pass through a message (skip a message from being produced). The @apidoc[ProducerMessage$] will pass through @apidoc[Producer.flowWithContext](Producer$) that will publish it to the Kafka Topic and finally we map the result to `Done`.

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #imports-producer #producerFlow }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #imports-producer #producerFlow }

The resulting flow is then used in the `atLeastOnceFlow` factory method to build the Projection.

Scala
:  @@snip [KafkaDocExample.scala](/examples/src/test/scala/docs/kafka/KafkaDocExample.scala) { #sendToKafkaProjectionFlow }

Java
:  @@snip [KafkaDocExample.java](/examples/src/test/java/jdocs/kafka/KafkaDocExample.java) { #sendToKafkaProjectionFlow }

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

## Configuration

Make your edits/overrides in your application.conf.

The reference configuration file with the default values:

@@snip [reference.conf](/akka-projection-kafka/src/main/resources/reference.conf) { #config }
