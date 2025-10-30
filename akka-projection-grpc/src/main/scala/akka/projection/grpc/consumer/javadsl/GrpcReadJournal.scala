/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.UUID
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.japi.Pair
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.LatestEventTimestampQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.projection.internal.CanTriggerReplay
import akka.stream.javadsl.Source
import com.google.protobuf.Descriptors

object GrpcReadJournal {
  val Identifier: String = scaladsl.GrpcReadJournal.Identifier

  /**
   * Construct a gRPC read journal from configuration `akka.projection.grpc.consumer`. The `stream-id` must
   * be defined in the configuration.
   *
   * Configuration from `akka.projection.grpc.consumer.client` will be used to connect to the remote producer.
   */
  def create(
      system: ClassicActorSystemProvider,
      protobufDescriptors: java.util.List[Descriptors.FileDescriptor]): GrpcReadJournal =
    create(system, GrpcQuerySettings(system), protobufDescriptors)

  /**
   * Construct a gRPC read journal for the given settings.
   *
   * Configuration from `akka.projection.grpc.consumer.client` will be used to connect to the remote producer.
   */
  def create(
      system: ClassicActorSystemProvider,
      settings: GrpcQuerySettings,
      protobufDescriptors: java.util.List[Descriptors.FileDescriptor]): GrpcReadJournal =
    create(
      system,
      settings,
      GrpcClientSettings.fromConfig(system.classicSystem.settings.config.getConfig(Identifier + ".client"))(system),
      protobufDescriptors)

  /**
   * Construct a gRPC read journal for the given settings and explicit `GrpcClientSettings` to control
   * how to reach the Akka Projection gRPC producer service (host, port etc).
   */
  def create(
      system: ClassicActorSystemProvider,
      settings: GrpcQuerySettings,
      clientSettings: GrpcClientSettings,
      protobufDescriptors: java.util.List[Descriptors.FileDescriptor]): GrpcReadJournal = {
    import scala.jdk.CollectionConverters._
    val protoAnySerialization =
      new ProtoAnySerialization(
        system.classicSystem.toTyped,
        protobufDescriptors.asScala.toVector,
        ProtoAnySerialization.Prefer.Java)
    new GrpcReadJournal(
      scaladsl
        .GrpcReadJournal(settings, clientSettings, protoAnySerialization, replicationSettings = None)(system))
  }

}

final class GrpcReadJournal(delegate: scaladsl.GrpcReadJournal)
    extends ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery
    with CanTriggerReplay
    with LatestEventTimestampQuery
    with CurrentEventsByPersistenceIdTypedQuery {

  /**
   * The identifier of the stream to consume, which is exposed by the producing/publishing side.
   * It is defined in the [[GrpcQuerySettings]].
   */
  def streamId(): String =
    delegate.streamId

  /**
   * Correlation id to be used with [[ConsumerFilter.ReplayWithFilter]].
   * Such replay request will trigger replay in all `eventsBySlices` queries
   * with the same `streamId` running from this instance of the `GrpcReadJournal`.
   * Create separate instances of the `GrpcReadJournal` to have separation between
   * replay requests for the same `streamId`.
   */
  val replayCorrelationId: UUID =
    delegate.replayCorrelationId

  @InternalApi
  private[akka] override def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit =
    delegate.triggerReplay(persistenceId, fromSeqNr, triggeredBySeqNr)

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import scala.jdk.CollectionConverters._
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate
      .timestampOf(persistenceId, sequenceNr)
      .map(_.toJava)(ExecutionContext.parasitic)
      .asJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).asJava

  override def latestEventTimestamp(
      entityType: String,
      minSlice: Int,
      maxSlice: Int): CompletionStage[Optional[Instant]] =
    delegate
      .latestEventTimestamp(entityType, minSlice, maxSlice)
      .map(_.toJava)(ExecutionContext.parasitic)
      .asJava

  override def currentEventsByPersistenceIdTyped[Event](
      persistenceId: String,
      fromSequenceNr: Long,
      toSequenceNr: Long): Source[EventEnvelope[Event], NotUsed] =
    delegate.currentEventsByPersistenceIdTyped(persistenceId, fromSequenceNr, toSequenceNr).asJava

  /**
   * Close the gRPC client. It will be automatically closed when the `ActorSystem` is terminated,
   * so invoking this is only needed when there is a need to close the resource before that.
   * After closing the `GrpcReadJournal` instance cannot be used again.
   */
  def close(): CompletionStage[Done] =
    delegate.close().asJava

}
