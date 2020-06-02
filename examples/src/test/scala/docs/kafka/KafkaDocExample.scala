/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.kafka

import scala.concurrent.Await
import akka.actor.typed.scaladsl._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.jdbc.scaladsl.JdbcHandler
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import jdocs.jpa.HibernateSessionProvider
import jdocs.jpa.HibernateSessionProvider.HibernateJdbcSession
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory

//#imports
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

//#imports

object KafkaDocExample {

  //#handler
  type Word = String
  type Count = Int

  class WordCountHandler(projectionId: ProjectionId)
      extends JdbcHandler[ConsumerRecord[String, String], HibernateJdbcSession] {
    private val logger = LoggerFactory.getLogger(getClass)
    private var state: Map[Word, Count] = Map.empty

    override def process(session: HibernateJdbcSession, envelope: ConsumerRecord[String, String]): Unit = {
      val word = envelope.value
      val newCount = state.getOrElse(word, 0) + 1
      logger.infoN(
        "{} consumed from topic/partition {}/{}. Word count for [{}] is {}",
        projectionId,
        envelope.topic,
        envelope.partition,
        word,
        newCount)
      state = state.updated(word, newCount)
    }
  }
  //#handler

  //#wordSource
  final case class WordEnvelope(offset: Long, word: Word)

  class WordSource(implicit ec: ExecutionContext) extends SourceProvider[Long, WordEnvelope] {

    private val src = Source(
      List(WordEnvelope(1L, "abc"), WordEnvelope(2L, "def"), WordEnvelope(3L, "ghi"), WordEnvelope(4L, "abc")))

    override def source(offset: () => Future[Option[Long]]): Future[Source[WordEnvelope, _]] = {
      offset()
        .map {
          case Some(o) => src.dropWhile(_.offset <= o)
          case _       => src
        }
        .map(_.throttle(1, 1.second))
    }

    override def extractOffset(env: WordEnvelope): Long = env.offset
  }
  //#wordSource

  //#wordPublisher
  class WordPublisher(topic: String, sendProducer: SendProducer[String, String])(implicit ec: ExecutionContext)
      extends JdbcHandler[WordEnvelope, HibernateJdbcSession] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(session: HibernateJdbcSession, envelope: WordEnvelope): Unit = {
      val word = envelope.word
      // using the word as the key and `DefaultPartitioner` will select partition based on the key
      // so that same word always ends up in same partition
      val key = word
      val producerRecord = new ProducerRecord(topic, key, word)
      val result = sendProducer.send(producerRecord).map { recordMetadata =>
        logger.infoN("Published word [{}] to topic/partition {}/{}", word, topic, recordMetadata.partition)
        Done
      }
      // FIXME support for async Handler, issue #23
      Await.result(result, 5.seconds)
    }
  }
  //#wordPublisher

  val config: Config = ConfigFactory.parseString("""
    akka.projection.slick = {

      profile = "slick.jdbc.H2Profile$"

      db = {
       url = "jdbc:h2:mem:test1"
       driver = org.h2.Driver
       connectionPool = disabled
       keepAliveConnection = true
      }
    }
    """)

  implicit lazy val system = ActorSystem[Guardian.Command](Guardian(), "Example", config)

  object IllustrateSourceProvider {

    //#sourceProvider
    val bootstrapServers = "localhost:9092"
    val groupId = "group-wordcount"
    val topicName = "words"
    val consumerSettings =
      ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers(bootstrapServers)
        .withGroupId(groupId)
        .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val sourceProvider: SourceProvider[GroupOffsets, ConsumerRecord[String, String]] =
      KafkaSourceProvider(system, consumerSettings, Set(topicName))
    //#sourceProvider
  }

  object IllustrateExactlyOnce {
    import IllustrateSourceProvider._

    //#exactlyOnce
    val sessionProvider = new HibernateSessionProvider

    val projectionId = ProjectionId("WordCount", "wordcount-1")
    val projection =
      JdbcProjection.exactlyOnce(
        projectionId,
        sourceProvider,
        () => sessionProvider.newInstance(),
        handler = () => new WordCountHandler(projectionId))
    //#exactlyOnce
  }

  object IllustrateSendingToKafka {

    implicit val ec = system.executionContext

    //#sendProducer
    val bootstrapServers = "localhost:9092"
    val topicName = "words"
    private val producerSettings =
      ProducerSettings(system, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)
    import akka.actor.typed.scaladsl.adapter._ // FIXME might not be needed in later Alpakka Kafka version?
    private val sendProducer = SendProducer(producerSettings)(system.toClassic)
    //#sendProducer

    //#sendToKafkaProjection
    val sourceProvider = new WordSource
    val sessionProvider = new HibernateSessionProvider

    val projectionId = ProjectionId("PublishWords", "words")
    val projection =
      JdbcProjection
        .exactlyOnce(
          projectionId,
          sourceProvider,
          () => sessionProvider.newInstance(),
          handler = () => new WordPublisher(topicName, sendProducer))

    //#sendToKafkaProjection

    // FIXME change above to atLeastOnce
  }

  def consumerProjection(n: Int): Projection[ConsumerRecord[String, String]] = {
    import IllustrateSourceProvider.sourceProvider
    val sessionProvider = new HibernateSessionProvider

    val projectionId = ProjectionId("WordCount", s"wordcount-$n")
    JdbcProjection.exactlyOnce(
      projectionId,
      sourceProvider,
      () => sessionProvider.newInstance(),
      handler = () => new WordCountHandler(projectionId))
  }

  def producerProjection(): Projection[WordEnvelope] = {
    IllustrateSendingToKafka.projection
  }

  object Guardian {
    sealed trait Command
    def apply(): Behavior[Command] = {
      Behaviors.setup { context =>
        context.spawn(ProjectionBehavior(consumerProjection(1)), "wordcount-1")
        context.spawn(ProjectionBehavior(consumerProjection(2)), "wordcount-2")
        context.spawn(ProjectionBehavior(consumerProjection(3)), "wordcount-3")

        context.spawn(ProjectionBehavior(producerProjection()), "wordPublisher")

        Behaviors.empty
      }

    }
  }

  /**
   * {{{
   * bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3 --topic words
   * bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic words
   *
   * sbt "examples/test:runMain docs.kafka.KafkaDocExample"
   *
   * bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic words --from-beginning
   *
   * }}}
   */
  def main(args: Array[String]): Unit = {
    system
  }

}
