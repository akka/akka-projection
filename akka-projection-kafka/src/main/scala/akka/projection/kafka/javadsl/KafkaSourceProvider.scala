/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.javadsl

import java.lang.{ Long => JLong }

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.kafka.ConsumerSettings
import akka.projection.MergeableOffset
import akka.projection.javadsl.SourceProvider
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.kafka.internal.KafkaSourceProviderSettings
import akka.projection.kafka.internal.MetadataClientAdapterImpl
import org.apache.kafka.clients.consumer.ConsumerRecord

@ApiMayChange
object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def create[K, V](
      system: ActorSystem[_],
      settings: ConsumerSettings[K, V],
      topics: java.util.Set[String]): SourceProvider[MergeableOffset[JLong], ConsumerRecord[K, V]] = {
    import akka.util.ccompat.JavaConverters._
    new KafkaSourceProviderImpl[K, V](
      system,
      settings,
      topics.asScala.toSet,
      () => new MetadataClientAdapterImpl(system, settings),
      KafkaSourceProviderSettings(system))
  }
}
