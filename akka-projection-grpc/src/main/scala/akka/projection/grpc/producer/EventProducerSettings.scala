/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import com.typesafe.config.Config

@ApiMayChange
object EventProducerSettings {
  def apply(system: ActorSystem[_]): EventProducerSettings =
    apply(system.settings.config.getConfig("akka.projection.grpc.producer"))

  def apply(config: Config): EventProducerSettings = {

    new EventProducerSettings(
      queryPluginId = config.getString("query-plugin-id"),
      transformationParallelism = config.getInt("transformation-parallelism"),
      replayParallelism = config.getInt("filter.replay-parallelism"))
  }
}

@ApiMayChange
final class EventProducerSettings private (
    val queryPluginId: String,
    val transformationParallelism: Int,
    val replayParallelism: Int) {
  require(transformationParallelism >= 1, "Configuration property [transformation-parallelism] must be >= 1.")
  require(replayParallelism >= 1, "Configuration property [replay-parallelism] must be >= 1.")

}
