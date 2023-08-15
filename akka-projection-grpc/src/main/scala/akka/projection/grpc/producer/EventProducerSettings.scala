/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.util.JavaDurationConverters.JavaDurationOps
import com.typesafe.config.Config

import scala.concurrent.duration.FiniteDuration

@ApiMayChange
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
      keepAliveInterval = config.getDuration("keep-alive-interval").asScala)
  }

  /** Java API */
  def create(system: ActorSystem[_]): EventProducerSettings =
    apply(system)

  /** Java API */
  def create(config: Config): EventProducerSettings =
    apply(config)
}

@ApiMayChange
final class EventProducerSettings private (
    val queryPluginId: String,
    val transformationParallelism: Int,
    val replayParallelism: Int,
    val topicTagPrefix: String,
    val keepAliveInterval: FiniteDuration) {
  require(transformationParallelism >= 1, "Configuration property [transformation-parallelism] must be >= 1.")
  require(replayParallelism >= 1, "Configuration property [replay-parallelism] must be >= 1.")

}
