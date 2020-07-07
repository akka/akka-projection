/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.HandlerRecoveryStrategy
import akka.projection.Projection
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final case class ProjectionSettings(
    restartBackoff: RestartBackoffSettings,
    saveOffsetAfterEnvelopes: Int,
    saveOffsetAfterDuration: FiniteDuration,
    groupAfterEnvelopes: Int,
    groupAfterDuration: FiniteDuration,
    recoveryStrategy: HandlerRecoveryStrategy)

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object ProjectionSettings {

  def apply(system: ActorSystem[_]): ProjectionSettings = {
    fromConfig(system.classicSystem.settings.config.getConfig("akka.projection"))
  }

  def fromConfig(config: Config): ProjectionSettings = {
    val restartBackoffConfig = config.getConfig("restart-backoff")
    val atLeastOnceConfig = config.getConfig("at-least-once")
    val groupedConfig = config.getConfig("grouped")
    val recoveryStrategyConfig = config.getConfig("recovery-strategy")
    new ProjectionSettings(
      RestartBackoffSettings(
        minBackoff = restartBackoffConfig.getDuration("min-backoff", MILLISECONDS).millis,
        maxBackoff = restartBackoffConfig.getDuration("max-backoff", MILLISECONDS).millis,
        randomFactor = restartBackoffConfig.getDouble("random-factor"),
        maxRestarts = restartBackoffConfig.getInt("max-restarts")),
      atLeastOnceConfig.getInt("save-offset-after-envelopes"),
      atLeastOnceConfig.getDuration("save-offset-after-duration", MILLISECONDS).millis,
      groupedConfig.getInt("group-after-envelopes"),
      groupedConfig.getDuration("group-after-duration", MILLISECONDS).millis,
      RecoveryStrategyConfig.fromConfig(recoveryStrategyConfig))
  }
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

/**
 * INTERNAL API
 */
@InternalApi private[projection] final case class RestartBackoffSettings(
    minBackoff: FiniteDuration,
    maxBackoff: FiniteDuration,
    randomFactor: Double,
    maxRestarts: Int)

/**
 * INTERNAL API: mixin to projection impl to not have to implement all overloaded variants in several places
 */
@InternalApi private[projection] trait SettingsImpl[ProjectionImpl <: Projection[_]] { self: ProjectionImpl =>
  def withRestartBackoffSettings(restartBackoff: RestartBackoffSettings): ProjectionImpl

  def withRestartBackoff(minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double): ProjectionImpl =
    withRestartBackoffSettings(RestartBackoffSettings(minBackoff, maxBackoff, randomFactor, -1))

  def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionImpl =
    withRestartBackoffSettings(RestartBackoffSettings(minBackoff, maxBackoff, randomFactor, maxRestarts))

  def withRestartBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double): ProjectionImpl =
    withRestartBackoffSettings(RestartBackoffSettings(minBackoff.asScala, maxBackoff.asScala, randomFactor, -1))

  def withRestartBackoff(
      minBackoff: java.time.Duration,
      maxBackoff: java.time.Duration,
      randomFactor: Double,
      maxRestarts: Int): ProjectionImpl =
    withRestartBackoffSettings(
      RestartBackoffSettings(minBackoff.asScala, maxBackoff.asScala, randomFactor, maxRestarts))

  def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): ProjectionImpl

  def withSaveOffset(afterEnvelopes: Int, afterDuration: java.time.Duration): ProjectionImpl =
    withSaveOffset(afterEnvelopes, afterDuration.asScala)

  def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): ProjectionImpl

  def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: java.time.Duration): ProjectionImpl =
    withGroup(groupAfterEnvelopes, groupAfterDuration.asScala)

}
