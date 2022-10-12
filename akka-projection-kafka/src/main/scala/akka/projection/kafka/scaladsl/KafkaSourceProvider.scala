/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.scaladsl

import java.lang.{ Long => JLong }

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import akka.projection.MergeableOffset
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.kafka.internal.KafkaSourceProviderSettings
import akka.projection.kafka.internal.MetadataClientAdapterImpl
import akka.projection.scaladsl.SourceProvider
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def apply[K, V](
      system: ActorSystem[_],
      settings: ConsumerSettings[K, V],
      topics: Set[String]): SourceProvider[MergeableOffset[JLong], ConsumerRecord[K, V]] =
    new KafkaSourceProviderImpl[K, V](
      system,
      settings,
      topics,
      () => new MetadataClientAdapterImpl(system, settings),
      KafkaSourceProviderSettings(system))
}
