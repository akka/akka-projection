/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.ConsumerSettings
import akka.kafka.Subscription
import akka.kafka.scaladsl.Consumer
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.common.TopicPartition

object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from Kafka managed offsets
   */
  def fromConsumerGroup[Key, ValueEnvelope](
      settings: ConsumerSettings[Key, ValueEnvelope],
      subscription: Subscription): SourceProvider[PartitionOffset, CommittableMessage[Key, ValueEnvelope]] =
    new KafkaOffsetSourceProvider[Key, ValueEnvelope](settings, subscription)

  /**
   * TODO
   *
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   *
   * NOTE: Uses Alpakka Kafka Consumer.plainPartitionedManualOffsetSource that merges all partitions back into
   * one stream
   */
  def fromOffsets[Key, ValueEnvelope](
      settings: ConsumerSettings[Key, ValueEnvelope],
      subscription: Subscription,
      offsets: Map[TopicPartition, Long]): SourceProvider[PartitionOffset, CommittableMessage[Key, ValueEnvelope]] =
    ???

  @InternalApi
  class KafkaOffsetSourceProvider[Key, ValueEnvelope] private[kafka] (
      settings: ConsumerSettings[Key, ValueEnvelope],
      subscription: Subscription)
      extends SourceProvider[PartitionOffset, CommittableMessage[Key, ValueEnvelope]] {

    // TODO: move optional offset to implementation constructors? not needed in this case because we're resuming from kafka committed offset
    override def source(notUsed: Option[PartitionOffset]): Source[CommittableMessage[Key, ValueEnvelope], _] = {
      Consumer
        .committableSource(settings, subscription)
    }

    override def extractOffset(envelope: CommittableMessage[Key, ValueEnvelope]): PartitionOffset =
      envelope.committableOffset.partitionOffset
  }
}
