/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.Done
import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.projection.scaladsl._

import scala.concurrent.Future


object KafkaProjections {

  def committableSource[K, V](settings: ConsumerSettings[K, V],
                              subscription: Subscription): KafkaProjectionBuilder[K, V] = {
    val sourceProvider = KafkaSourceProviders.committableSource(settings, subscription)
    KafkaProjectionBuilder(sourceProvider)
  }
}


case class KafkaProjectionBuilder[K, V](sourceProvider: SourceProvider[CommittableOffset, CommittableMessage[K, V]],
                                        runner: ProjectionRunner[CommittableOffset, Future[Done]] = KafkaProjectionRunners.atLeastOnceRunner) {

  def withAtLeastOnce = this.copy(runner = KafkaProjectionRunners.atLeastOnceRunner)

  def withAtMostOnce = this.copy(runner = KafkaProjectionRunners.atMostOnceRunner)

  def withEventHandler(handler: ProjectionHandler[V, Future[Done]]): Projection[CommittableMessage[K, V], V, CommittableOffset, Future[Done]] = {
    Projection(
      sourceProvider,
      KafkaExtractors.committableSource[K, V],
      runner,
      handler
    )
  }

}