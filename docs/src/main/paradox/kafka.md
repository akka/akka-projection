# Messages from Kafka

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

## Dependencies

To use the Kafka module of Akka Projections add the following dependency in your project:

@@dependency [sbt,Maven,Gradle] {
  group=com.typesafe.akka
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
