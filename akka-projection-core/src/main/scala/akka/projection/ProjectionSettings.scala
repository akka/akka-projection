/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import java.time

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

trait ProjectionSettings {

  def minBackoff: FiniteDuration
  def maxBackoff: FiniteDuration
  def randomFactor: Double

  def withBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomBackoffFactor: Double): ProjectionSettings

  /**
   * Java API
   */
  def withBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomBackoffFactor: Double): ProjectionSettings
}

object ProjectionSettings {

  /**
   * Java API
   */
  def create(system: ClassicActorSystemProvider): ProjectionSettings = apply(system)

  def apply(system: ClassicActorSystemProvider): ProjectionSettings = {
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection.restart-backoff"))
  }

  def fromConfig(config: Config) =
    new ProjectionSettingsImpl(
      config.getDuration("min-backoff", MILLISECONDS).millis,
      config.getDuration("max-backoff", MILLISECONDS).millis,
      config.getDouble("random-factor"))

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class ProjectionSettingsImpl(
    val minBackoff: FiniteDuration,
    val maxBackoff: FiniteDuration,
    val randomFactor: Double)
    extends ProjectionSettings {

  /**
   * Scala API
   */
  override def withBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomBackoffFactor: Double): ProjectionSettings =
    new ProjectionSettingsImpl(minBackoff, maxBackoff, randomBackoffFactor)

  /**
   * Java API
   */
  override def withBackoff(
      minBackoff: time.Duration,
      maxBackoff: time.Duration,
      randomBackoffFactor: Double): ProjectionSettings =
    new ProjectionSettingsImpl(minBackoff.asScala, maxBackoff.asScala, randomBackoffFactor)
}
