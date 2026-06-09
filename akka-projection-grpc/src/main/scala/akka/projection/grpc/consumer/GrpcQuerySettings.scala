/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer

import akka.actor.ClassicActorSystemProvider
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
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

    val keepAliveInterval = config.getDuration("keep-alive-interval").toScala
    val keepAliveTimeout = config.getDuration("keep-alive-timeout").toScala
    val keepAliveFailureThreshold = config.getInt("keep-alive-failure-threshold")

    new GrpcQuerySettings(
      streamId,
      additionalHeaders,
      Vector.empty,
      None,
      keepAliveInterval,
      keepAliveTimeout,
      keepAliveFailureThreshold)
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
      keepAliveInterval = scala.concurrent.duration.Duration.Zero,
      keepAliveTimeout = scala.concurrent.duration.Duration.Zero,
      keepAliveFailureThreshold = 3)
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
      keepAliveInterval = scala.concurrent.duration.Duration.Zero,
      keepAliveTimeout = scala.concurrent.duration.Duration.Zero,
      keepAliveFailureThreshold = 3)
  }
}

final class GrpcQuerySettings private (
    val streamId: String,
    val additionalRequestMetadata: Option[Metadata],
    val initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria],
    val fromReplica: Option[ReplicaId],
    val keepAliveInterval: FiniteDuration,
    val keepAliveTimeout: FiniteDuration,
    val keepAliveFailureThreshold: Int) {
  require(
    streamId != "",
    "streamId must be an id exposed by the producing side but was undefined on the consuming side.")
  require(keepAliveFailureThreshold >= 1, s"keepAliveFailureThreshold must be >= 1, was [$keepAliveFailureThreshold]")

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
   * Enable akka-projection-grpc level keep alive on the eventsBySlices stream. The consumer
   * sends a keep-alive request every `interval` and verifies that the producer's response
   * arrives within `timeout`, exercising the gRPC stream and the producer's stream handler
   * rather than just the TCP connection. The stream fails after `failureThreshold`
   * consecutive overdue responses (earlier ones log a warning), letting brief network spikes
   * or a slow producer recover.
   *
   * Set `timeout` to `Duration.Zero` to keep sending requests without ever failing the
   * stream (firewall keep-alive only).
   *
   * The deadline measures Pong observation at the consumer-side stage. Pongs share the
   * inbound channel with Events, so a consumer-side downstream that stalls pulling events
   * for longer than `timeout * failureThreshold` can also trigger failure even when the
   * producer is responsive. Pick `timeout` and `failureThreshold` with that in mind.
   *
   * Requires an akka-projection producer of version 1.6.24 or newer.
   */
  def withKeepAlive(interval: FiniteDuration, timeout: FiniteDuration, failureThreshold: Int): GrpcQuerySettings =
    copy(keepAliveInterval = interval, keepAliveTimeout = timeout, keepAliveFailureThreshold = failureThreshold)

  /**
   * Java API: see the Scala `withKeepAlive` overload.
   */
  def withKeepAlive(
      interval: java.time.Duration,
      timeout: java.time.Duration,
      failureThreshold: Int): GrpcQuerySettings =
    copy(
      keepAliveInterval = interval.toScala,
      keepAliveTimeout = timeout.toScala,
      keepAliveFailureThreshold = failureThreshold)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def withFromReplica(replica: ReplicaId): GrpcQuerySettings =
    copy(fromReplica = Option(replica))

  private def copy(
      streamId: String = streamId,
      additionalRequestMetadata: Option[Metadata] = additionalRequestMetadata,
      initialConsumerFilter: immutable.Seq[ConsumerFilter.FilterCriteria] = initialConsumerFilter,
      fromReplica: Option[ReplicaId] = fromReplica,
      keepAliveInterval: FiniteDuration = keepAliveInterval,
      keepAliveTimeout: FiniteDuration = keepAliveTimeout,
      keepAliveFailureThreshold: Int = keepAliveFailureThreshold): GrpcQuerySettings =
    new GrpcQuerySettings(
      streamId,
      additionalRequestMetadata,
      initialConsumerFilter,
      fromReplica,
      keepAliveInterval,
      keepAliveTimeout,
      keepAliveFailureThreshold)

}
