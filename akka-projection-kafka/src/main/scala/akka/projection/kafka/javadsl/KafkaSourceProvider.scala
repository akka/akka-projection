/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.javadsl

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.kafka.internal.MetadataClientAdapterImpl
import akka.projection.javadsl
import akka.projection.kafka.internal.KafkaSourceProviderSettings
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[KafkaSourceProvider]] that resumes from externally managed offsets
   */
  def create[K, V](
      system: ActorSystem[_],
      settings: ConsumerSettings[K, V],
      topics: java.util.Set[String]): KafkaSourceProvider[K, V] = {
    import akka.util.ccompat.JavaConverters._
    new KafkaSourceProviderImpl[K, V](
      system,
      settings,
      topics.asScala.toSet,
      new MetadataClientAdapterImpl(system, settings),
      KafkaSourceProviderSettings(system),
      readOffsetDelay = None)
  }

}

abstract class KafkaSourceProvider[K, V]
    extends javadsl.SourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with javadsl.VerifiableSourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with javadsl.MergeableOffsetSourceProvider[GroupOffsets.TopicPartitionKey, GroupOffsets, ConsumerRecord[K, V]] {
  def withReadOffsetDelay(delay: java.time.Duration): KafkaSourceProvider[K, V]
}
