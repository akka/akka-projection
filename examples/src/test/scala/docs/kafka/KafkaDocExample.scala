/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.kafka

import java.lang.{ Long => JLong }

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.projection.MergeableOffset
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.jdbc.scaladsl.JdbcHandler
import akka.projection.jdbc.scaladsl.JdbcProjection
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import javax.persistence.EntityManager
import jdocs.jdbc.HibernateJdbcSession
import jdocs.jdbc.HibernateSessionFactory
import org.slf4j.LoggerFactory

// #imports-producer
import org.apache.kafka.common.serialization.StringSerializer
import akka.kafka.ProducerSettings
// #imports-producer

//#imports
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
//#imports

// #sendProducer
import akka.kafka.scaladsl.SendProducer
// #sendProducer

// #producerFlow
import org.apache.kafka.clients.producer.ProducerRecord
import akka.kafka.ProducerMessage
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.FlowWithContext
import akka.projection.ProjectionContext

// #producerFlow

object KafkaDocExample {

  //#wordSource
  type Word = String
  type Count = Int
  //#wordSource

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

  //#wordSource
  final case class WordEnvelope(offset: Long, word: Word)

  class WordSource(implicit ec: ExecutionContext) extends SourceProvider[Long, WordEnvelope] {

    private val src = Source(
      List(WordEnvelope(1L, "abc"), WordEnvelope(2L, "def"), WordEnvelope(3L, "ghi"), WordEnvelope(4L, "abc")))

    override def source(offset: () => Future[Option[Long]]): Future[Source[WordEnvelope, NotUsed]] = {
      offset()
        .map {
          case Some(o) => src.dropWhile(_.offset <= o)
          case _       => src
        }
        .map(_.throttle(1, 1.second))
    }

    override def extractOffset(env: WordEnvelope): Long = env.offset

    override def extractCreationTime(env: WordEnvelope): Long = 0L
  }
  //#wordSource

  //#wordPublisher
  class WordPublisher(topic: String, sendProducer: SendProducer[String, String])(implicit ec: ExecutionContext)
      extends Handler[WordEnvelope] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: WordEnvelope): Future[Done] = {
      val word = envelope.word
      // using the word as the key and `DefaultPartitioner` will select partition based on the key
      // so that same word always ends up in same partition
      val key = word
      val producerRecord = new ProducerRecord(topic, key, word)
      val result = sendProducer.send(producerRecord).map { recordMetadata =>
        logger.infoN("Published word [{}] to topic/partition {}/{}", word, topic, recordMetadata.partition)
        Done
      }
      result
    }
  }
  //#wordPublisher

  val config: Config = ConfigFactory.parseString("""
    akka.projection.jdbc = {
      dialect = "h2-dialect"
      blocking-jdbc-dispatcher {
        type = Dispatcher
        executor = "thread-pool-executor"
        thread-pool-executor {
          fixed-pool-size = 10
        }
        throughput = 1
      }
    
      offset-store {
        schema = ""
        table = "AKKA_PROJECTION_OFFSET_STORE"
      }
    
      debug.verbose-offset-store-logging = false
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

    val sourceProvider: SourceProvider[MergeableOffset[JLong], ConsumerRecord[String, String]] =
      KafkaSourceProvider(system, consumerSettings, Set(topicName))
    //#sourceProvider
  }

  object IllustrateExactlyOnce {
    import IllustrateSourceProvider._

    val wordRepository: WordRepository = null
    //#exactlyOnce
    val sessionProvider = new HibernateSessionFactory

    val projectionId = ProjectionId("WordCount", "wordcount-1")
    val projection =
      JdbcProjection.exactlyOnce(
        projectionId,
        sourceProvider,
        () => sessionProvider.newInstance(),
        handler = () => new WordCountJdbcHandler(wordRepository))
    //#exactlyOnce

    // #exactly-once-jdbc-handler
    class WordCountJdbcHandler(val wordRepository: WordRepository)
        extends JdbcHandler[ConsumerRecord[String, String], HibernateJdbcSession] {

      @throws[Exception]
      override def process(session: HibernateJdbcSession, envelope: ConsumerRecord[String, String]): Unit = {
        val word = envelope.value
        wordRepository.increment(session.entityManager, word)
      }
    }

    // #exactly-once-jdbc-handler

    // #repository
    trait WordRepository {
      def increment(entityManager: EntityManager, word: String): Unit
    }
    // #repository

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
    val sessionProvider = new HibernateSessionFactory

    val projectionId = ProjectionId("PublishWords", "words")
    val projection =
      JdbcProjection
        .atLeastOnceAsync(
          projectionId,
          sourceProvider,
          () => sessionProvider.newInstance(),
          handler = () => new WordPublisher(topicName, sendProducer))

    //#sendToKafkaProjection

  }

  object IllustrateSendingToKafkaUsingFlow {

    implicit val ec = system.executionContext

    //#producerFlow
    val bootstrapServers = "localhost:9092"
    val topicName = "words"

    private val producerSettings =
      ProducerSettings(system, new StringSerializer, new StringSerializer)
        .withBootstrapServers(bootstrapServers)

    val producerFlow =
      FlowWithContext[WordEnvelope, ProjectionContext]
        .map(wordEnv => ProducerMessage.single(new ProducerRecord(topicName, wordEnv.word, wordEnv.word)))
        .via(Producer.flowWithContext(producerSettings))
        .map(_ => Done)
    //#producerFlow

    //#sendToKafkaProjectionFlow
    val sourceProvider = new WordSource
    val sessionProvider = new HibernateSessionFactory

    val projectionId = ProjectionId("PublishWords", "words")
    val projection =
      JdbcProjection
        .atLeastOnceFlow(projectionId, sourceProvider, () => sessionProvider.newInstance(), producerFlow)
    //#sendToKafkaProjectionFlow

  }

  def consumerProjection(n: Int): Projection[ConsumerRecord[String, String]] = {
    import IllustrateSourceProvider.sourceProvider
    val sessionProvider = new HibernateSessionFactory

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
    val sessionProvider = new HibernateSessionFactory
    JdbcProjection.createOffsetTableIfNotExists(() => sessionProvider.newInstance())
    system
  }

}
