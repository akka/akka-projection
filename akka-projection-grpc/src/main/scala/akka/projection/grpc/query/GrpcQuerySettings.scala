/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.query

import com.typesafe.config.Config

object GrpcQuerySettings {
  def apply(config: Config): GrpcQuerySettings =
    new GrpcQuerySettings(config)
}

class GrpcQuerySettings(config: Config) {
  val grpcClientConfig: Config = config.getConfig("client")

  val protoClassMapping: Map[String, String] = {
    import scala.jdk.CollectionConverters._
    config.getConfig("proto-class-mapping").root.unwrapped.asScala.toMap.map {
      case (k, v) => k -> v.toString
    }

  }
}
