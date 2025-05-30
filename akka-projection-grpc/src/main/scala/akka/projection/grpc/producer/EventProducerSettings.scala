/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters._

object EventProducerSettings {

  /** Scala API */
  def apply(system: ActorSystem[_]): EventProducerSettings =
    apply(system.settings.config.getConfig("akka.projection.grpc.producer"))

  /** Scala API */
  def apply(config: Config): EventProducerSettings = {
    new EventProducerSettings(
      queryPluginId = config.getString("query-plugin-id"),
      transformationParallelism = config.getInt("transformation-parallelism"),
      replayParallelism = config.getInt("filter.replay-parallelism"),
      topicTagPrefix = config.getString("filter.topic-tag-prefix"),
      keepAliveInterval = config.getDuration("keep-alive-interval").toScala,
      akkaSerializationOnly = false)
  }

  /** Java API */
  def create(system: ActorSystem[_]): EventProducerSettings =
    apply(system)

  /** Java API */
  def create(config: Config): EventProducerSettings =
    apply(config)
}

final class EventProducerSettings private (
    val queryPluginId: String,
    val transformationParallelism: Int,
    val replayParallelism: Int,
    val topicTagPrefix: String,
    val keepAliveInterval: FiniteDuration,
    val akkaSerializationOnly: Boolean) {
  require(transformationParallelism >= 1, "Configuration property [transformation-parallelism] must be >= 1.")
  require(replayParallelism >= 1, "Configuration property [replay-parallelism] must be >= 1.")

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def withAkkaSerializationOnly(): EventProducerSettings = {
    new EventProducerSettings(
      queryPluginId,
      transformationParallelism,
      replayParallelism,
      topicTagPrefix,
      keepAliveInterval,
      akkaSerializationOnly = true)
  }

}
