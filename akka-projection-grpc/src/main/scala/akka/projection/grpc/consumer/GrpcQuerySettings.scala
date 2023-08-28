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

import scala.collection.immutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

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

    new GrpcQuerySettings(streamId, additionalHeaders, Vector.empty)
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
    new GrpcQuerySettings(streamId, additionalRequestMetadata = None, initialConsumerFilter = Vector.empty)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume. It is exposed by the producing side.
   */
  def create(streamId: String): GrpcQuerySettings = {
    new GrpcQuerySettings(streamId, additionalRequestMetadata = None, initialConsumerFilter = Vector.empty)
  }
}

@ApiMayChange
final class GrpcQuerySettings private (
    val streamId: String,
    val additionalRequestMetadata: Option[Metadata],
    val initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria]) {
  require(
    streamId != "",
    "streamId must be an id exposed by the producing side but was undefined on the consuming side.")

  /**
   * Additional request metadata, for authentication/authorization of the request on the remote side.
   */
  def withAdditionalRequestMetadata(metadata: Metadata): GrpcQuerySettings =
    copy(additionalRequestMetadata = Some(metadata))

  /**
   * Scala API: Set the initial consumer filter to use for events
   */
  def withInitialConsumerFilter(
      initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria]): GrpcQuerySettings =
    copy(initialConsumerFilter = initialConsumerFilter)

  /**
   * Java API: Set the initial consumer filter to use for events
   */
  def withInitialConsumerFilter(
      initialConsumerFilter: java.util.List[ConsumerFilter.FilterCriteria]): GrpcQuerySettings =
    copy(initialConsumerFilter = initialConsumerFilter.asScala.toVector)

  private def copy(
      streamId: String = streamId,
      additionalRequestMetadata: Option[Metadata] = additionalRequestMetadata,
      initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria] = initialConsumerFilter): GrpcQuerySettings =
    new GrpcQuerySettings(streamId, additionalRequestMetadata, initialConsumerFilter)

}
