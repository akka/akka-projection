/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import scala.concurrent.Future

import akka.annotation.InternalApi
import akka.kafka.AutoSubscription
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.projection.internal.MergeableOffsets
import akka.projection.internal.MergeableOffsets.OffsetRow
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object KafkaSourceProvider {

  private val regexTp = """(.+)-(\d+)""".r

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def apply[Key, ValueEnvelope](
      settings: ConsumerSettings[Key, ValueEnvelope],
      subscription: AutoSubscription): SourceProvider[MergeableOffsets.Offset, ConsumerRecord[Key, ValueEnvelope]] =
    new KafkaOffsetSourceProvider[Key, ValueEnvelope](settings, subscription)

  @InternalApi
  class KafkaOffsetSourceProvider[Key, ValueEnvelope] private[kafka] (
      settings: ConsumerSettings[Key, ValueEnvelope],
      // all Alpakka Source's need a subscription, but we could generate a topic subscription automatically based on the provided offset.
      // if the offset is None though we're dead in the water
      subscription: AutoSubscription)
      extends SourceProvider[MergeableOffsets.Offset, ConsumerRecord[Key, ValueEnvelope]] {

    override def source(offsets: Option[MergeableOffsets.Offset]): Source[ConsumerRecord[Key, ValueEnvelope], _] = {
      offsets match {
        case Some(mergeableOffsets) =>
          val getOffsetsOnAssign = (tps: Set[TopicPartition]) =>
            Future.successful {
              (mergeableOffsets.entries
                .map { entry =>
                  val tp = entry.name match {
                    case regexTp(topic, partition) => new TopicPartition(topic, partition.toInt)
                    case _ =>
                      throw new IllegalArgumentException(
                        s"Row entry name (${entry.name}) must match pattern: ${regexTp.pattern.toString}")
                  }
                  tp -> entry.offset
                }
                .filter {
                  case (tp, _) => tps.contains(tp)
                })
                .toMap
            }

          val numPartitions = mergeableOffsets.entries.size
          Consumer
            .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign)
            .flatMapMerge(numPartitions, {
              case (_, partitionedSource) => partitionedSource
            })
        case _ =>
          // plainSource, start offsets based on "auto.offset.reset" consumer property
          Consumer.plainSource(settings, subscription)
      }
    }

    override def extractOffset(envelope: ConsumerRecord[Key, ValueEnvelope]): MergeableOffsets.Offset = {
      val name = envelope.topic() + "-" + envelope.partition()
      MergeableOffsets.one(OffsetRow(name, envelope.offset()))
    }
  }
}
