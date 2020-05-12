/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import java.time

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
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
  def groupAfterEnvelopes: Int
  def groupAfterDuration: FiniteDuration
  def recoveryStrategy: HandlerRecoveryStrategy

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
  def create(system: ActorSystem[_]): ProjectionSettings = apply(system)

  def apply(system: ActorSystem[_]): ProjectionSettings = {
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection"))
  }

  def fromConfig(config: Config): ProjectionSettings = {
    val restartBackoffConfig = config.getConfig("restart-backoff")
    val atLeastOnceConfig = config.getConfig("at-least-once")
    val groupedConfig = config.getConfig("grouped")
    val recoveryStrategyConfig = config.getConfig("recovery-strategy")
    new ProjectionSettingsImpl(
      restartBackoffConfig.getDuration("min-backoff", MILLISECONDS).millis,
      restartBackoffConfig.getDuration("max-backoff", MILLISECONDS).millis,
      restartBackoffConfig.getDouble("random-factor"),
      restartBackoffConfig.getInt("max-restarts"),
      atLeastOnceConfig.getInt("save-offset-after-envelopes"),
      atLeastOnceConfig.getDuration("save-offset-after-duration", MILLISECONDS).millis,
      groupedConfig.getInt("group-after-envelopes"),
      groupedConfig.getDuration("group-after-duration", MILLISECONDS).millis,
      RecoveryStrategyConfig.fromConfig(recoveryStrategyConfig))
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
    val saveOffsetAfterDuration: FiniteDuration,
    val groupAfterEnvelopes: Int,
    val groupAfterDuration: FiniteDuration,
    val recoveryStrategy: HandlerRecoveryStrategy)
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
      saveOffsetAfterDuration: FiniteDuration = saveOffsetAfterDuration,
      recoveryStrategy: HandlerRecoveryStrategy = recoveryStrategy): ProjectionSettings =
    new ProjectionSettingsImpl(
      minBackoff,
      maxBackoff,
      randomFactor,
      maxRestarts,
      saveOffsetAfterEnvelopes,
      saveOffsetAfterDuration,
      groupAfterEnvelopes,
      groupAfterDuration,
      recoveryStrategy)
}

private object RecoveryStrategyConfig {
  def fromConfig(config: Config): HandlerRecoveryStrategy = {
    val strategy = config.getString("strategy")
    val retries = config.getInt("retries")
    val retryDelay = config.getDuration("retry-delay", MILLISECONDS).millis

    strategy match {
      case "fail"           => HandlerRecoveryStrategy.fail
      case "skip"           => HandlerRecoveryStrategy.skip
      case "retry-and-fail" => HandlerRecoveryStrategy.retryAndFail(retries, retryDelay)
      case "retry-and-skip" => HandlerRecoveryStrategy.retryAndSkip(retries, retryDelay)
      case s =>
        throw new IllegalArgumentException(
          s"Strategy type [$s] is not supported. Supported options are [fail, skip, retry-and-fail, retry-and-skip]")
    }
  }
}
