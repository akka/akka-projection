package akka.projection

import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.kafka.{ CommitterSettings, ConsumerSettings, Subscriptions }
import akka.kafka.scaladsl.{ Committer, Consumer }
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{ EventEnvelope, Offset, PersistenceQuery, TimeBasedUUID }
import akka.projection.KafkaAtLeastOnceSample.projection
import akka.projection.KafkaSourceWithTransactionalJDBCSample.{ projection, system }
import akka.projection.scaladsl.jdbc.{ JdbcOffsetStore, JdbcOffsetStoreAkkaOffset, JdbcOffsetStoreLong, JdbcProjection }
import akka.projection.scaladsl.kafka.{ KafkaConsumer, KafkaProjection }
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future

/**
 * A Kafka projection that commits the offset in the topic using the Committer.flow (at-least-once)
 */
object KafkaAtLeastOnceSample {
  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  val consumerSettings: ConsumerSettings[String, String] = ???
  val committerSettings = CommitterSettings(system).withMaxBatch(100)

  val src = KafkaConsumer.atLeastOnce(
    consumerSettings.withGroupId("group1"),
    Subscriptions.topics("topic1"),
    committerSettings) { msg =>
    Future.successful("pretending that this is projected to somewhere")
  }

  // a pure Kafka projection is super simple, as it only requires a Source
  val projection = KafkaProjection(src)
  projection.start()
  projection.stop()
}

object KafkaAtLeastOnceSample2 {
  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  val consumerSettings: ConsumerSettings[String, String] = ???
  val committerSettings = CommitterSettings(system).withMaxBatch(100)

  val src = Consumer
    .committableSource(consumerSettings, Subscriptions.topics("topic1"))
    .mapAsync(1) { msg =>
      Future.successful(s"pretending that this is projected to somewhere").map(_ => msg.committableOffset)
    }
    .via(Committer.flow(committerSettings))

  // a pure Kafka projection is super simple, as it only requires a Source
  val projection = KafkaProjection(src)
  projection.start()
  projection.stop()
}

/**
 * A Kafka projection that commits the offset in the topic before delivering to handler (at-most-once)
 */
object KafkaAtMostOnceSample {

  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  val consumerSettings: ConsumerSettings[String, String] = ???

  val src =
    Consumer.atMostOnceSource(consumerSettings.withGroupId("group1"), Subscriptions.topics("topic1")).mapAsync(1) {
      msg =>
        println(s"pretending that this is projected to somewhere")
        Future.successful(Done)
    }

  // a pure Kafka projection is super simple, as it only requires a Source
  val projection = KafkaProjection(src)
  projection.start()
  projection.stop()
}

/**
 * Sample showing a rough API for consuming events from Akka Persistence Cassandra and saving the
 * projected model and the offset in a JDBC table
 */
object AkkaPersistenceCassandraWithTransactionalJDBCSample {

  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  // we need a function that given an optional offset, returns a Source
  def sourceProvider(offset: Option[Offset]): Source[EventEnvelope, _] = {
    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    queries.eventsByTag("someTag", offset.getOrElse(Offset.noOffset))
  }

  val projection =
    JdbcProjection.transactional(
      sourceProvider = sourceProvider,
      offsetStore = new JdbcOffsetStoreAkkaOffset(projectionId = "demo"),
      offsetExtractor = (ev: EventEnvelope) => ev.offset) { (connec, envelope) =>
      println(s"pretending that this is projected to some JDBC model")
    }

  projection.start()
  projection.stop()
}

/**
 * sample showing a rough API for consuming events from Akka Persistence JDBC and saving the
 * projected model and the offset in a JDBC table
 */
object AkkaPersistenceJdbcWithTransactionalJDBCSample {

  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  // we need a function that given an optional offset, returns a Source
  def sourceProvider(offset: Option[Offset]): Source[EventEnvelope, _] = {
    val queries = PersistenceQuery(system).readJournalFor[JdbcReadJournal](JdbcReadJournal.Identifier)
    queries.eventsByTag("someTag", offset.getOrElse(Offset.noOffset))
  }

  val projection =
    JdbcProjection.transactional(
      sourceProvider = sourceProvider,
      offsetStore = new JdbcOffsetStoreAkkaOffset(projectionId = "demo"),
      offsetExtractor = (ev: EventEnvelope) => ev.offset) { (connec, envelope) =>
      println(s"pretending that this is projected to some JDBC model")
    }

  projection.start()
  projection.stop()
}

/**
 * sample showing a rough API for consuming events from Alpakka Kafka and saving the
 * projected model and the offset in a JDBC table
 */
object KafkaSourceWithTransactionalJDBCSample {

  implicit val system = ActorSystem("sample")
  implicit val ec = system.dispatcher

  // we need a function that given an optional offset, returns a Source
  def sourceProvider(offset: Option[Long]): Source[ConsumerRecord[String, String], _] = {
    val consumerSettings: ConsumerSettings[String, String] = ???
    val topicPartition: TopicPartition = ???
    Consumer.plainSource(
      consumerSettings,
      Subscriptions.assignmentWithOffset(topicPartition -> offset.map(_ + 1).getOrElse(0L)))
  }

  val projection =
    JdbcProjection.transactional(
      sourceProvider = sourceProvider,
      offsetStore = new JdbcOffsetStoreLong(projectionId = "demo"),
      offsetExtractor = (record: ConsumerRecord[String, String]) => record.offset()) { (connec, envelope) =>
      // whatever is
      println(s"pretending that this is projected to some JDBC model")
    }
  projection.start()
}
