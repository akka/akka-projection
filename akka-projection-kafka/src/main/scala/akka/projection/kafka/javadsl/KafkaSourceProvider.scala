/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.javadsl

import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.internal.KafkaSourceProviderImpl
import akka.projection.kafka.internal.MetadataClientAdapterImpl
import akka.projection.javadsl.SourceProvider
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaSourceProvider {

  /**
   * Create a [[SourceProvider]] that resumes from externally managed offsets
   */
  def create[K, V](
      system: ActorSystem[_],
      settings: ConsumerSettings[K, V],
      topics: java.util.Set[String]): SourceProvider[GroupOffsets, ConsumerRecord[K, V]] = {
    import scala.collection.JavaConverters._
    new KafkaSourceProviderImpl[K, V](
      system,
      settings,
      topics.asScala.toSet,
      new MetadataClientAdapterImpl(system, settings))
  }

}
