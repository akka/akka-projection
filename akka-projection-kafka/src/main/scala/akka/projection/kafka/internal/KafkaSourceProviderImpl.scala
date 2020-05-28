/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.kafka.ConsumerSettings
import akka.kafka.KafkaConsumerActor
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.MetadataClient
import akka.projection.MergeableOffset
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KafkaSourceProviderImpl {
  private type ReadOffsets = () => Future[Option[MergeableOffset[Long]]]
  private val RegexTp = """(.+)-(\d+)""".r
  private val KafkaMetadataTimeout = 10.seconds // TODO: get from config

  private val consumerActorNameCounter = new AtomicInteger
  private def nextConsumerActorName(): String =
    s"kafkaSourceProviderConsumer-${consumerActorNameCounter.incrementAndGet()}"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class KafkaSourceProviderImpl[K, V](
    system: ActorSystem[_],
    settings: ConsumerSettings[K, V],
    topics: Set[String])
    extends SourceProvider[MergeableOffset[Long], ConsumerRecord[K, V]] {
  import KafkaSourceProviderImpl._

  private val classicSystem = system.classicSystem.asInstanceOf[ExtendedActorSystem]
  private implicit val executionContext: ExecutionContext = system.executionContext

  private val subscription = Subscriptions.topics(topics)
  private lazy val consumerActor =
    classicSystem.systemActorOf(KafkaConsumerActor.props(settings), nextConsumerActorName())
  private lazy val metadataClient = MetadataClient.create(consumerActor, KafkaMetadataTimeout)(executionContext)

  private def stopMetadataClient(): Unit = {
    metadataClient.close()
    classicSystem.stop(consumerActor)
  }

  override def source(readOffsets: ReadOffsets): Future[Source[ConsumerRecord[K, V], _]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
    val numPartitionsF = Future.sequence(topics.map(metadataClient.getPartitionsFor)).map(_.map(_.length).sum)
    numPartitionsF.failed.foreach(_ => stopMetadataClient())
    numPartitionsF.map { numPartitions =>
      Consumer
        .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign(readOffsets))
        .flatMapMerge(numPartitions, {
          case (_, partitionedSource) => partitionedSource
        })
        .watchTermination()(Keep.right)
        .mapMaterializedValue { terminated =>
          terminated.onComplete(_ => stopMetadataClient())
        }
    }
  }

  override def extractOffset(record: ConsumerRecord[K, V]): MergeableOffset[Long] = {
    val key = record.topic() + "-" + record.partition()
    MergeableOffset(Map(key -> record.offset()))
  }

  private def getOffsetsOnAssign(readOffsets: ReadOffsets): Set[TopicPartition] => Future[Map[TopicPartition, Long]] =
    (assignedTps: Set[TopicPartition]) =>
      readOffsets()
        .flatMap {
          case Some(mergeableOffsets) =>
            Future.successful(mergeableOffsets.entries.flatMap {
              case (surrogateProjectionKey, offset) =>
                val tp = parseProjectionKey(surrogateProjectionKey)
                if (assignedTps.contains(tp)) Map(tp -> offset)
                else Map.empty
            })
          case None => metadataClient.getBeginningOffsets(assignedTps)
        }
        .recover {
          case NonFatal(ex) => throw new RuntimeException("External offsets could not be retrieved", ex)
        }

  private def parseProjectionKey(surrogateProjectionKey: String): TopicPartition = {
    surrogateProjectionKey match {
      case RegexTp(topic, partition) => new TopicPartition(topic, partition.toInt)
      case _ =>
        throw new IllegalArgumentException(
          s"Row entry name (${surrogateProjectionKey}) must match pattern: ${RegexTp.pattern.toString}")
    }
  }
}
