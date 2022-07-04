/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.actor.typed.ActorSystem
import com.typesafe.config.Config

object EventProducerSettings {
  def apply(system: ActorSystem[_]): EventProducerSettings =
    apply(system.settings.config.getConfig("akka.projection.grpc.producer"))

  def apply(config: Config): EventProducerSettings =
    new EventProducerSettings(config)
}

class EventProducerSettings(config: Config) {
  val queryPluginId: String = config.getString("query-plugin-id")

  val transformationParallelism: Int =
    config.getInt("transformation-parallelism")
  require(
    transformationParallelism >= 1,
    "Configuration property [transformation-parallelism] must be >= 1.")

}
