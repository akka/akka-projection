/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.scaladsl

import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.kafka.internal.KafkaSourceProviderSettings
import akka.projection.kafka.internal.MetadataClientAdapterImpl
import akka.projection.scaladsl
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[KafkaSourceProvider]] that resumes from externally managed offsets
   */
  def apply[K, V](
      system: ActorSystem[_],
      settings: ConsumerSettings[K, V],
      topics: Set[String]): KafkaSourceProvider[K, V] =
    new KafkaSourceProviderImpl[K, V](
      system,
      settings,
      topics,
      new MetadataClientAdapterImpl(system, settings),
      KafkaSourceProviderSettings(system),
      readOffsetDelay = None)

}

trait KafkaSourceProvider[K, V]
    extends scaladsl.SourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.VerifiableSourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.MergeableOffsetSourceProvider[GroupOffsets.TopicPartitionKey, GroupOffsets, ConsumerRecord[K, V]] {
  def withReadOffsetDelay(delay: FiniteDuration): KafkaSourceProvider[K, V]
}
