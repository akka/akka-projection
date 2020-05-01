/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.MetadataClient
import akka.projection.internal.MergeableOffsets
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object KafkaSourceProvider {
  private val regexTp = """(.+)-(\d+)""".r

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def apply[K, V](
      systemProvider: ClassicActorSystemProvider,
      settings: ConsumerSettings[K, V],
      topics: Set[String]): SourceProvider[MergeableOffsets.Offset[Long], ConsumerRecord[K, V]] =
    new KafkaOffsetSourceProvider[K, V](systemProvider, settings, topics)

  @InternalApi
  class KafkaOffsetSourceProvider[K, V] private[kafka] (
      systemProvider: ClassicActorSystemProvider,
      settings: ConsumerSettings[K, V],
      topics: Set[String])
      extends SourceProvider[MergeableOffsets.Offset[Long], ConsumerRecord[K, V]] {
    implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

    private val subscription = Subscriptions.topics(topics)
    // TODO: configurable timeout
    private val metadataClient = MetadataClient.create(settings, 10.seconds)(systemProvider.classicSystem, dispatcher)

    override def source(
        readOffsets: () => Future[Option[MergeableOffsets.Offset[Long]]]): Future[Source[ConsumerRecord[K, V], _]] = {
      val getOffsetsOnAssign: Set[TopicPartition] => Future[Map[TopicPartition, Long]] =
        (assignedTps: Set[TopicPartition]) => {
          readOffsets()
            .flatMap {
              case Some(mergeableOffsets) =>
                Future.successful {
                  mergeableOffsets.entries
                    .map {
                      case (surrogateProjectionKey, offset) =>
                        val tp = surrogateProjectionKey match {
                          case regexTp(topic, partition) => new TopicPartition(topic, partition.toInt)
                          case _ =>
                            throw new IllegalArgumentException(
                              s"Row entry name (${surrogateProjectionKey}) must match pattern: ${regexTp.pattern.toString}")
                        }
                        tp -> offset
                    }
                    .filter {
                      case (tp, _) => assignedTps.contains(tp)
                    }
                }
              // TODO: should we let the user decide if they want to start from beginning or end offsets?
              case None => metadataClient.getBeginningOffsets(assignedTps)
            }
            .recover {
              case NonFatal(ex) => throw new RuntimeException("External offsets could not be retrieved", ex)
            }
        }

      // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
      // number.  i don't think using a large number would present a problem.
      val numPartitionsF = Future.sequence(topics.map(metadataClient.getPartitionsFor)).map(_.map(_.length).sum)
      numPartitionsF.map { numPartitions =>
        Consumer
          .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign)
          .flatMapMerge(numPartitions, {
            case (_, partitionedSource) => partitionedSource
          })
      }
    }

    override def extractOffset(envelope: ConsumerRecord[K, V]): MergeableOffsets.Offset[Long] = {
      val key = envelope.topic() + "-" + envelope.partition()
      MergeableOffsets.Offset(Map(key -> envelope.offset()))
    }
  }
}
