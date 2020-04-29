/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.annotation.InternalApi
import akka.kafka.ConsumerMessage.PartitionOffset
import akka.kafka.ConsumerSettings
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from Kafka managed offsets
   */
  // FIXME
//  def fromConsumerGroup[Key, ValueEnvelope](
//      settings: ConsumerSettings[Key, ValueEnvelope],
//      subscription: Subscription): SourceProvider[PartitionOffset, CommittableMessage[Key, ValueEnvelope]] =
//    new KafkaOffsetSourceProvider[Key, ValueEnvelope](settings, subscription)

  /**
   * TODO
   *
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   *
   * NOTE: Uses Alpakka Kafka Consumer.plainPartitionedManualOffsetSource that merges all partitions back into
   * one stream
   */
  def fromOffsets[Key, ValueEnvelope](settings: ConsumerSettings[Key, ValueEnvelope])
      : SourceProvider[PartitionOffset, ConsumerRecord[Key, ValueEnvelope]] =
    new KafkaOffsetSourceProvider[Key, ValueEnvelope](settings)

  @InternalApi
  class KafkaOffsetSourceProvider[Key, ValueEnvelope] private[kafka] (settings: ConsumerSettings[Key, ValueEnvelope])
      extends SourceProvider[PartitionOffset, ConsumerRecord[Key, ValueEnvelope]] {

    // TODO: move optional offset to implementation constructors? not needed in this case because we're resuming from kafka committed offset
    override def source(offset: Option[PartitionOffset]): Source[ConsumerRecord[Key, ValueEnvelope], _] = {
      // Consumer.plainSource
      ???
    }

    override def extractOffset(envelope: ConsumerRecord[Key, ValueEnvelope]): PartitionOffset =
      ???
  }
}
