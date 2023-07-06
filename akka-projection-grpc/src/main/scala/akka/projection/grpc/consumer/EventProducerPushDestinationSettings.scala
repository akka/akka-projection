/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.actor.typed.ActorSystem
import akka.util.JavaDurationConverters.JavaDurationOps
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

object EventProducerPushDestinationSettings {
  def apply(system: ActorSystem[_]): EventProducerPushDestinationSettings = {
    apply(system.settings.config.getConfig("akka.projection.grpc.consumer.push-destination"))
  }

  def apply(config: Config): EventProducerPushDestinationSettings = {
    new EventProducerPushDestinationSettings(
      parallelism = config.getInt("parallelism"),
      journalWriteTimeout = config.getDuration("journal-write-timeout").asScala)
  }

  def apply(parallelism: Int, journalWriteTimeout: FiniteDuration): EventProducerPushDestinationSettings =
    new EventProducerPushDestinationSettings(parallelism, journalWriteTimeout)
}

final class EventProducerPushDestinationSettings private (val parallelism: Int, val journalWriteTimeout: FiniteDuration)
