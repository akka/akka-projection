/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer

import akka.actor.ClassicActorSystemProvider
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.internal.ConfigHelpers
import akka.projection.grpc.internal.KeepAliveSettings
import com.typesafe.config.Config
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

import akka.annotation.InternalApi
import akka.persistence.typed.ReplicaId

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

    new GrpcQuerySettings(
      streamId,
      additionalRequestMetadata = additionalHeaders,
      initialConsumerFilter = Vector.empty,
      fromReplica = None,
      keepAliveInterval = ConfigHelpers.optionalDuration(config, "keep-alive-interval"),
      keepAliveTimeout = ConfigHelpers.optionalDuration(config, "keep-alive-timeout"))
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
    new GrpcQuerySettings(
      streamId,
      additionalRequestMetadata = None,
      initialConsumerFilter = Vector.empty,
      fromReplica = None,
      keepAliveInterval = None,
      keepAliveTimeout = None)
  }

  /**
   * Java API: Programmatic construction of GrpcQuerySettings
   *
   * @param streamId The stream id to consume. It is exposed by the producing side.
   */
  def create(streamId: String): GrpcQuerySettings = {
    new GrpcQuerySettings(
      streamId,
      additionalRequestMetadata = None,
      initialConsumerFilter = Vector.empty,
      fromReplica = None,
      keepAliveInterval = None,
      keepAliveTimeout = None)
  }
}

final class GrpcQuerySettings private (
    val streamId: String,
    val additionalRequestMetadata: Option[Metadata],
    val initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria],
    val fromReplica: Option[ReplicaId],
    val keepAliveInterval: Option[FiniteDuration],
    val keepAliveTimeout: Option[FiniteDuration]) {
  require(
    streamId != "",
    "streamId must be an id exposed by the producing side but was undefined on the consuming side.")
  KeepAliveSettings.validate(keepAliveInterval, keepAliveTimeout)

  /**
   * Additional request metadata, for authentication/authorization of the request on the remote side.
   */
  def withAdditionalRequestMetadata(metadata: Metadata): GrpcQuerySettings =
    copy(additionalRequestMetadata = Some(metadata))

  /**
   * Scala API: Set the initial consumer filter to use for events. Should only be used for static, up front consumer filters.
   * Combining this with updating consumer filters directly means that the filters may be reset to these filters on GrpcReadJournal creation.
   */
  def withInitialConsumerFilter(
      initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria]): GrpcQuerySettings =
    copy(initialConsumerFilter = initialConsumerFilter)

  /**
   * Java API: Set the initial consumer filter to use for events. Should only be used for static, up front consumer filters.
   * Combining this with updating consumer filters directly means that the filters may be reset to these filters on GrpcReadJournal creation.
   */
  def withInitialConsumerFilter(
      initialConsumerFilter: java.util.List[ConsumerFilter.FilterCriteria]): GrpcQuerySettings =
    copy(initialConsumerFilter = initialConsumerFilter.asScala.toVector)

  /**
   * Scala API: Request that the producer emits keep alive messages at this interval when there are no events,
   * to keep the event stream connection alive end-to-end through proxies and load balancers. Must be at least
   * 1 second. The producer must support keep alives (akka-projection 1.6.26 or later), older producers will
   * ignore the request.
   */
  def withKeepAliveInterval(interval: FiniteDuration): GrpcQuerySettings =
    copy(keepAliveInterval = Some(interval))

  /**
   * Java API: Request that the producer emits keep alive messages at this interval when there are no events,
   * to keep the event stream connection alive end-to-end through proxies and load balancers. Must be at least
   * 1 second. The producer must support keep alives (akka-projection 1.6.26 or later), older producers will
   * ignore the request.
   */
  def withKeepAliveInterval(interval: java.time.Duration): GrpcQuerySettings =
    copy(keepAliveInterval = Some(interval.toScala))

  /**
   * Scala API: Fail the event stream if neither events nor keep alive messages have been received within this
   * timeout. Can only be used after a keep alive interval has been set. This is an idle timeout on the stream,
   * so it should be several times the keep alive interval and longer than the slowest expected projection handler
   * or batch, otherwise the stream is restarted while the handler is busy. Note that the stream will also fail
   * if the producer does not support keep alives.
   */
  def withKeepAliveTimeout(timeout: FiniteDuration): GrpcQuerySettings =
    copy(keepAliveTimeout = Some(timeout))

  /**
   * Java API: Fail the event stream if neither events nor keep alive messages have been received within this
   * timeout. Can only be used after a keep alive interval has been set. This is an idle timeout on the stream,
   * so it should be several times the keep alive interval and longer than the slowest expected projection handler
   * or batch, otherwise the stream is restarted while the handler is busy. Note that the stream will also fail
   * if the producer does not support keep alives.
   */
  def withKeepAliveTimeout(timeout: java.time.Duration): GrpcQuerySettings =
    copy(keepAliveTimeout = Some(timeout.toScala))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def withFromReplica(replica: ReplicaId): GrpcQuerySettings =
    copy(fromReplica = Option(replica))

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def withKeepAlive(
      interval: Option[FiniteDuration],
      timeout: Option[FiniteDuration]): GrpcQuerySettings =
    copy(keepAliveInterval = interval, keepAliveTimeout = timeout)

  private def copy(
      streamId: String = streamId,
      additionalRequestMetadata: Option[Metadata] = additionalRequestMetadata,
      initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria] = initialConsumerFilter,
      fromReplica: Option[ReplicaId] = fromReplica,
      keepAliveInterval: Option[FiniteDuration] = keepAliveInterval,
      keepAliveTimeout: Option[FiniteDuration] = keepAliveTimeout): GrpcQuerySettings =
    new GrpcQuerySettings(
      streamId,
      additionalRequestMetadata,
      initialConsumerFilter,
      fromReplica,
      keepAliveInterval,
      keepAliveTimeout)

}
