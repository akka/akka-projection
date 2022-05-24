/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.query

import com.typesafe.config.Config

object GrpcQuerySettings {
  def apply(config: Config): GrpcQuerySettings =
    new GrpcQuerySettings(config)
}

class GrpcQuerySettings(config: Config) {}
