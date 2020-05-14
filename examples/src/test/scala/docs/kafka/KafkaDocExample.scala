/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.kafka

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Random

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import akka.projection.Projection
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.SlickHandler
import akka.projection.slick.SlickProjection
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.H2Profile

//#imports
import akka.projection.kafka.KafkaSourceProvider
import akka.projection.MergeableOffset
import akka.kafka.ConsumerSettings
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer

//#imports

object KafkaDocExample {

  //#handler
  type Word = String
  type Count = Int

  class WordCountHandler(projectionId: ProjectionId) extends SlickHandler[ConsumerRecord[String, String]] {
    private val logger = LoggerFactory.getLogger(getClass)
    private var state: Map[Word, Count] = Map.empty

    override def process(envelope: ConsumerRecord[String, String]): DBIO[Done] = {
      val word = envelope.value
      val newCount = state.getOrElse(word, 0) + 1
      logger.info(
        "{} consumed from topic/partition {}/{}. Word count for [{}] is {}",
        projectionId,
        envelope.topic,
        envelope.partition,
        word,
        newCount)
      state = state.updated(word, newCount)
      DBIO.successful(Done)
    }
  }
  //#handler

  //#wordSource
  final case class WordEnvelope(offset: Long, word: Word)

  class WordSource(implicit ec: ExecutionContext) extends SourceProvider[Long, WordEnvelope] {

    private val words = Vector("aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff", "gggg")
    private val rnd = new Random(17) // same sequence each time
    private val src = Source
      .fromIterator(() =>
        new Iterator[WordEnvelope] {
          private var offset = 0L
          override def hasNext: Boolean = true

          override def next(): WordEnvelope = {
            val i = rnd.nextInt(words.size)
            offset += 1
            WordEnvelope(offset, words(i))
          }
        })
      .throttle(1, 1.second)

    override def source(offset: () => Future[Option[Long]]): Future[Source[WordEnvelope, _]] = {
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }
    }

    override def extractOffset(env: WordEnvelope): Long = env.offset
  }
  //#wordSource

  //#wordPublisher
  class WordPublisher(topic: String, sendProducer: SendProducer[String, String])(implicit ec: ExecutionContext)
      extends SlickHandler[WordEnvelope] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: WordEnvelope): DBIO[Done] = {
      val word = envelope.word
      // using the word as the key and `DefaultPartitioner` will select partition based on the key
      // so that same word always ends up in same partition
      val key = word
      val producerRecord = new ProducerRecord(topic, key, word)
      val result = sendProducer.send(producerRecord).map { recordMetadata =>
        logger.info("Published word [{}] to topic/partition {}/{}", word, topic, recordMetadata.partition)
        Done
      }
      DBIO.from(result)
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

    val sourceProvider: SourceProvider[MergeableOffset[Long], ConsumerRecord[String, String]] =
      KafkaSourceProvider(system, consumerSettings, Set(topicName))
    //#sourceProvider
  }

  object IllustrateExactlyOnce {
    import IllustrateSourceProvider._

    //#exactlyOnce
    val databaseConfig: DatabaseConfig[H2Profile] =
      DatabaseConfig.forConfig("akka.projection.slick", system.settings.config)
    val projectionId = ProjectionId("WordCount", "wordcount-1")
    val projection =
      SlickProjection.exactlyOnce(
        projectionId,
        sourceProvider,
        databaseConfig,
        handler = new WordCountHandler(projectionId))
    //#exactlyOnce

    projection.createOffsetTableIfNotExists()
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

    val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", system.settings.config)
    val projectionId = ProjectionId("PublishWords", "words")
    val projection =
      SlickProjection.atLeastOnce(
        projectionId,
        sourceProvider,
        dbConfig,
        handler = new WordPublisher(topicName, sendProducer),
        saveOffsetAfterEnvelopes = 100,
        saveOffsetAfterDuration = 500.millis)
    //#sendToKafkaProjection

    projection.createOffsetTableIfNotExists()
  }

  def consumerProjection(n: Int): Projection[ConsumerRecord[String, String]] = {
    import IllustrateSourceProvider.sourceProvider
    import IllustrateExactlyOnce.databaseConfig

    val projectionId = ProjectionId("WordCount", s"wordcount-$n")
    SlickProjection.exactlyOnce(
      projectionId,
      sourceProvider,
      databaseConfig,
      handler = new WordCountHandler(projectionId))
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
