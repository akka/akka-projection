/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

import java.time.{ Duration => JDuration }

object EventProducerPushDestinationSettings {

  /**
   * Scala API
   */
  def apply(system: ActorSystem[_]): EventProducerPushDestinationSettings = {
    apply(system.settings.config.getConfig("akka.projection.grpc.consumer.push-destination"))
  }

  /**
   * Scala API
   */
  def apply(config: Config): EventProducerPushDestinationSettings = {
    new EventProducerPushDestinationSettings(
      parallelism = config.getInt("parallelism"),
      journalWriteTimeout = config.getDuration("journal-write-timeout").toScala)
  }

  /**
   * Scala API
   */
  def apply(parallelism: Int, journalWriteTimeout: FiniteDuration): EventProducerPushDestinationSettings =
    new EventProducerPushDestinationSettings(parallelism, journalWriteTimeout)

  /**
   * Java API
   */
  def create(system: ActorSystem[_]): EventProducerPushDestinationSettings = apply(system)

  /**
   * Java API
   */
  def create(config: Config): EventProducerPushDestinationSettings = apply(config)

  /**
   * Java API
   */
  def create(parallelism: Int, journalWriteTimeout: JDuration): EventProducerPushDestinationSettings =
    apply(parallelism, journalWriteTimeout.toScala)
}

final class EventProducerPushDestinationSettings private (val parallelism: Int, val journalWriteTimeout: FiniteDuration)
