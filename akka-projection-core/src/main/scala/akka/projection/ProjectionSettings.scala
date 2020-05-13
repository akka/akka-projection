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
  def maxRestarts: Int

  def withBackoff(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double): ProjectionSettings

  def withBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionSettings

  /**
   * Java API
   */
  def withBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): ProjectionSettings

  /**
   * Java API
   */
  def withBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionSettings
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
      config.getDouble("random-factor"),
      config.getInt("max-restarts"))

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class ProjectionSettingsImpl(
    val minBackoff: FiniteDuration,
    val maxBackoff: FiniteDuration,
    val randomFactor: Double,
    val maxRestarts: Int)
    extends ProjectionSettings {

  /**
   * Scala API
   */
  override def withBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): ProjectionSettings =
    withBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts)

  override def withBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionSettings =
    new ProjectionSettingsImpl(minBackoff, maxBackoff, randomFactor, maxRestarts)

  /**
   * Java API
   */
  override def withBackoff(
      minBackoff: time.Duration,
      maxBackoff: time.Duration,
      randomFactor: Double): ProjectionSettings =
    withBackoff(minBackoff, maxBackoff, randomFactor, maxRestarts = -1)

  /**
   * Java API
   */
  override def withBackoff(
      minBackoff: time.Duration,
      maxBackoff: time.Duration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionSettings =
    new ProjectionSettingsImpl(minBackoff.asScala, maxBackoff.asScala, randomFactor, maxRestarts)
}
