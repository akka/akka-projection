/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.Done
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{ConsumerSettings, Subscription}
import akka.projection.scaladsl._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.concurrent.Future


object KafkaProjections {

  def committableSource[K, V](settings: ConsumerSettings[K, V],
                              subscription: Subscription): CommittableSourceProjectionBuilderStage1[K, V] = {
    val sourceProvider = KafkaSourceProviders.committableSource(settings, subscription)
    CommittableSourceProjectionBuilderStage1(sourceProvider)
  }

  def plainSource[K, V](settings: ConsumerSettings[K, V],
                        topicPartition: TopicPartition): PlainSourceProjectionBuilderStage1[K, V] = {
    val sourceProvider = KafkaSourceProviders.plainSource(settings, topicPartition)
    PlainSourceProjectionBuilderStage1(sourceProvider)
  }
}

final case class CommittableSourceProjectionBuilderStage1[K, V](private val sourceProvider: SourceProvider[CommittableOffset, CommittableMessage[K, V]]) {

  def withEventHandler(handler: EventHandler[V, Future[Done]]): CommittableSourceProjectionBuilderStage2[K, V] =
    CommittableSourceProjectionBuilderStage2(sourceProvider, handler)
}

final case class CommittableSourceProjectionBuilderStage2[K, V](private val sourceProvider: SourceProvider[CommittableOffset, CommittableMessage[K, V]],
                                                                handler: EventHandler[V, Future[Done]]) {

  def withAtLeastOnce: Projection[CommittableMessage[K, V], V, CommittableOffset, Future[Done]] =
    Projection(
      sourceProvider,
      KafkaExtractors.committableMessage[K, V],
      KafkaProjectionRunners.atLeastOnceRunner,
      handler
    )

  def withAtMostOnce: Projection[CommittableMessage[K, V], V, CommittableOffset, Future[Done]] =
    Projection(
      sourceProvider,
      KafkaExtractors.committableMessage[K, V],
      KafkaProjectionRunners.atMostOnceRunner,
      handler
    )

}

final case class PlainSourceProjectionBuilderStage1[K, V](private val sourceProvider: SourceProvider[Long, ConsumerRecord[K, V]]) {

  def withEventHandler[R](handler: EventHandler[V, R]): PlainSourceProjectionBuilderStage2[K, V, R] =
    PlainSourceProjectionBuilderStage2(sourceProvider, handler)
}

final case class PlainSourceProjectionBuilderStage2[K, V, R](private val sourceProvider: SourceProvider[Long, ConsumerRecord[K, V]],
                                                             private val handler: EventHandler[V, R]) {

  def withProjectionRunner(runner: ProjectionRunner[Long, R]): Projection[ConsumerRecord[K, V], V, Long, R] = {
    Projection(
      sourceProvider,
      KafkaExtractors.consumerRecover[K, V],
      runner,
      handler
    )
  }

}