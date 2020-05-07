/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.actor.ClassicActorSystemProvider
import akka.kafka.ConsumerSettings
import akka.projection.internal.MergeableOffset
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.scaladsl.SourceProvider
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def apply[K, V](
      systemProvider: ClassicActorSystemProvider,
      settings: ConsumerSettings[K, V],
      topics: Set[String]): SourceProvider[MergeableOffset[Long], ConsumerRecord[K, V]] =
    new KafkaSourceProviderImpl[K, V](systemProvider, settings, topics)
}
