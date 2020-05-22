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
  def saveOffsetAfterEnvelopes: Int
  def saveOffsetAfterDuration: FiniteDuration

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
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection"))
  }

  def fromConfig(config: Config) = {
    val restartBackoffConfig = config.getConfig("restart-backoff")
    val atLeastOnceConfig = config.getConfig("at-least-once")
    new ProjectionSettingsImpl(
      restartBackoffConfig.getDuration("min-backoff", MILLISECONDS).millis,
      restartBackoffConfig.getDuration("max-backoff", MILLISECONDS).millis,
      restartBackoffConfig.getDouble("random-factor"),
      restartBackoffConfig.getInt("max-restarts"),
      atLeastOnceConfig.getInt("save-offset-after-envelopes"),
      atLeastOnceConfig.getDuration("save-offset-after-duration", MILLISECONDS).millis)
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class ProjectionSettingsImpl(
    val minBackoff: FiniteDuration,
    val maxBackoff: FiniteDuration,
    val randomFactor: Double,
    val maxRestarts: Int,
    val saveOffsetAfterEnvelopes: Int,
    val saveOffsetAfterDuration: FiniteDuration)
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
    copy(minBackoff = minBackoff, maxBackoff = maxBackoff, randomFactor = randomFactor, maxRestarts = maxRestarts)

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
    copy(
      minBackoff = minBackoff.asScala,
      maxBackoff = maxBackoff.asScala,
      randomFactor = randomFactor,
      maxRestarts = maxRestarts)

  private[akka] def copy(
      minBackoff: FiniteDuration = minBackoff,
      maxBackoff: FiniteDuration = maxBackoff,
      randomFactor: Double = randomFactor,
      maxRestarts: Int = maxRestarts,
      saveOffsetAfterEnvelopes: Int = saveOffsetAfterEnvelopes,
      saveOffsetAfterDuration: FiniteDuration = saveOffsetAfterDuration): ProjectionSettings =
    new ProjectionSettingsImpl(
      minBackoff,
      maxBackoff,
      randomFactor,
      maxRestarts,
      saveOffsetAfterEnvelopes,
      saveOffsetAfterDuration)
}
