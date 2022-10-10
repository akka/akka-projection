/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import java.util.Optional

import scala.compat.java8.OptionConverters._

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
   * @param streamId                  The stream id to consume
   * @param additionalRequestMetadata Additional request metadata, for authentication/authorization of the request
   *                                  on the remote side.
   */
  def apply(streamId: String, additionalRequestMetadata: Option[Metadata]): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume
   * @param additionalRequestMetadata Additional request metadata, for authentication/authorization of the request
   *                              on the remote side.
   */
  def create(streamId: String, additionalRequestMetadata: Optional[akka.grpc.javadsl.Metadata]): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata.asScala.map(_.asScala))
  }
}

@ApiMayChange
final class GrpcQuerySettings(val streamId: String, val additionalRequestMetadata: Option[Metadata]) {
  require(
    streamId != "",
    "Configuration property [stream-id] must be an id exposed by the streaming side (but was empty).")

}
