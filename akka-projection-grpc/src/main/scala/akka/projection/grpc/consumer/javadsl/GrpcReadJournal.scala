/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import java.time.Instant
import java.util
import java.util.Optional
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._

import akka.Done
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.grpc.GrpcClientSettings
import akka.japi.Pair
import akka.persistence.query.Offset
import akka.persistence.query.javadsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.javadsl.EventTimestampQuery
import akka.persistence.query.typed.javadsl.EventsBySliceQuery
import akka.persistence.query.typed.javadsl.LoadEventQuery
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.stream.javadsl.Source
import com.google.protobuf.Descriptors

@ApiMayChange
object GrpcReadJournal {
  val Identifier: String = scaladsl.GrpcReadJournal.Identifier

  /**
   * Construct a gRPC read journal from configuration `akka.projection.grpc.consumer`. The `stream-id` must
   * be defined in the configuration.
   */
  def create(
      system: ClassicActorSystemProvider,
      protobufDescriptors: java.util.List[Descriptors.FileDescriptor]): GrpcReadJournal =
    create(
      system,
      GrpcQuerySettings(system),
      GrpcClientSettings.fromConfig(system.classicSystem.settings.config.getConfig(Identifier + ".client"))(system),
      protobufDescriptors)

  /**
   * Construct a gRPC read journal for the given stream-id and explicit `GrpcClientSettings` to control
   * how to reach the Akka Projection gRPC producer service (host, port etc).
   */
  def create(
      system: ClassicActorSystemProvider,
      settings: GrpcQuerySettings,
      clientSettings: GrpcClientSettings,
      protobufDescriptors: java.util.List[Descriptors.FileDescriptor]): GrpcReadJournal = {
    import akka.util.ccompat.JavaConverters._
    new GrpcReadJournal(scaladsl
      .GrpcReadJournal(settings, clientSettings, protobufDescriptors.asScala.toList, ProtoAnySerialization.Prefer.Java)(
        system))
  }

}

@ApiMayChange
class GrpcReadJournal(delegate: scaladsl.GrpcReadJournal)
    extends ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery {

  /**
   * The identifier of the stream to consume, which is exposed by the producing/publishing side.
   * It is defined in the [[GrpcQuerySettings]].
   */
  def streamId(): String =
    delegate.streamId

  override def eventsBySlices[Event](
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Event], NotUsed] =
    delegate.eventsBySlices(entityType, minSlice, maxSlice, offset).asJava

  override def sliceForPersistenceId(persistenceId: String): Int =
    delegate.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): util.List[Pair[Integer, Integer]] = {
    import akka.util.ccompat.JavaConverters._
    delegate
      .sliceRanges(numberOfRanges)
      .map(range => Pair(Integer.valueOf(range.min), Integer.valueOf(range.max)))
      .asJava
  }

  override def timestampOf(persistenceId: String, sequenceNr: Long): CompletionStage[Optional[Instant]] =
    delegate
      .timestampOf(persistenceId, sequenceNr)
      .map(_.asJava)(ExecutionContexts.parasitic)
      .toJava

  override def loadEnvelope[Event](persistenceId: String, sequenceNr: Long): CompletionStage[EventEnvelope[Event]] =
    delegate.loadEnvelope[Event](persistenceId, sequenceNr).toJava

  /**
   * Close the gRPC client. It will be automatically closed when the `ActorSystem` is terminated,
   * so invoking this is only needed when there is a need to close the resource before that.
   * After closing the `GrpcReadJournal` instance cannot be used again.
   */
  def close(): CompletionStage[Done] =
    delegate.close().toJava
}
