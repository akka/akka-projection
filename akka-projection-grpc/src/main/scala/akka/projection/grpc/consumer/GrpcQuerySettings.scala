/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import com.typesafe.config.Config

@ApiMayChange
object GrpcQuerySettings {
  def apply(config: Config): GrpcQuerySettings = {
    val streamId = config.getString("stream-id")
    require(
      streamId != "",
      "Configuration property [stream-id] must be an id exposed by the producing side but was undefined on the consuming side.")

    val additionalHeaders: Option[Metadata] = {
      import scala.jdk.CollectionConverters._
      val map = config.getConfig("additional-request-headers").root.unwrapped.asScala.toMap.map {
        case (k, v) => k -> v.toString
      }
      if (map.isEmpty) None
      else
        Some(
          map
            .foldLeft(new MetadataBuilder()) {
              case (builder, (key, value)) =>
                builder.addText(key, value)
            }
            .build())
    }

    new GrpcQuerySettings(streamId, additionalHeaders)
  }

  /**
   * Scala API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume
   */
  def apply(streamId: String): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata = None)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume
   */
  def create(streamId: String): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata = None)
  }
}

@ApiMayChange
final class GrpcQuerySettings private (val streamId: String, val additionalRequestMetadata: Option[Metadata]) {
  require(
    streamId != "",
    "streamId must be an id exposed by the producing side but was undefined on the consuming side.")

  /**
   * Additional request metadata, for authentication/authorization of the request on the remote side.
   */
  def withAdditionalRequestMetadata(metadata: Metadata): GrpcQuerySettings =
    new GrpcQuerySettings(streamId, Option(metadata))

}
