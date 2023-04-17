/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import com.typesafe.config.Config

@ApiMayChange
object GrpcQuerySettings {

  /**
   * Scala API: From `Config` `akka.projection.grpc.consumer` configuration section.
   */
  def apply(system: ClassicActorSystemProvider): GrpcQuerySettings =
    apply(system.classicSystem.settings.config.getConfig(GrpcReadJournal.Identifier))

  /**
   * Scala API: From `Config` corresponding to `akka.projection.grpc.consumer` configuration section.
   */
  def apply(config: Config): GrpcQuerySettings = {
    val streamId = config.getString("stream-id")
    require(
      streamId != "",
      "Configuration property [stream-id] must be an id exposed by the producing side but was undefined on the consuming side.")

    val additionalHeaders: Option[Metadata] = {
      import akka.util.ccompat.JavaConverters._
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
   * Java API: From `Config` `akka.projection.grpc.consumer` configuration section.
   */
  def create(system: ClassicActorSystemProvider): GrpcQuerySettings =
    apply(system)

  /**
   * Java API: From `Config` corresponding to `akka.projection.grpc.consumer` configuration section.
   */
  def create(config: Config): GrpcQuerySettings =
    apply(config)

  /**
   * Scala API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume. It is exposed by the producing side.
   */
  def apply(streamId: String): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata = None)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume. It is exposed by the producing side.
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
