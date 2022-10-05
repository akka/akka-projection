/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import com.typesafe.config.Config
import akka.util.ccompat.JavaConverters._

import java.util.Optional
import scala.compat.java8.OptionConverters._

@ApiMayChange
object GrpcQuerySettings {
  def apply(config: Config): GrpcQuerySettings = {
    val streamId = config.getString("stream-id")
    require(
      streamId != "",
      "Configuration property [stream-id] must be an id exposed by the streaming side (but was empty).")

    val protoClassMapping: Map[String, String] = {
      import scala.jdk.CollectionConverters._
      config.getConfig("proto-class-mapping").root.unwrapped.asScala.toMap.map {
        case (k, v) => k -> v.toString
      }

    }

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

    new GrpcQuerySettings(streamId, protoClassMapping, additionalHeaders)
  }

  /**
   * Scala API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId                  The stream id to consume
   * @param protoClassMapping         Mapping between full Protobuf message names and Java class names that are used
   *                                  when deserializing Protobuf events.
   * @param additionalRequestMetadata Additional request metadata, for authentication/authorization of the request
   *                                  on the remote side.
   */
  def apply(
      streamId: String,
      protoClassMapping: Map[String, String],
      additionalRequestMetadata: Option[Metadata]): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, protoClassMapping, additionalRequestMetadata)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume
   * @param protoClassMapping Mapping between full Protobuf message names and Java class names that are used
   *                          when deserializing Protobuf events.
   * @param additionalRequestMetadata Additional request metadata, for authentication/authorization of the request
   *                              on the remote side.
   */
  def create(
      streamId: String,
      protoClassMapping: java.util.Map[String, String],
      additionalRequestMetadata: Optional[akka.grpc.javadsl.Metadata]): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, protoClassMapping.asScala.toMap, additionalRequestMetadata.asScala.map(_.asScala))
  }
}

@ApiMayChange
final class GrpcQuerySettings(
    val streamId: String,
    val protoClassMapping: Map[String, String],
    val additionalRequestMetadata: Option[Metadata]) {
  require(
    streamId != "",
    "Configuration property [stream-id] must be an id exposed by the streaming side (but was empty).")

}
