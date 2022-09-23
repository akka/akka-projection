/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.collection.immutable
import scala.concurrent.Future
import akka.NotUsed
import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter._
import akka.grpc.GrpcClientSettings
import akka.persistence.Persistence
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl._
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.EventTimestampQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.LoadEventQuery
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.projection.grpc.internal.proto
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventProducerServiceClient
import akka.projection.grpc.internal.proto.EventTimestampRequest
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.LoadEventRequest
import akka.projection.grpc.internal.proto.LoadEventResponse
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Source
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object GrpcReadJournal {
  val Identifier = "akka.projection.grpc.consumer"

  private val log: Logger =
    LoggerFactory.getLogger(classOf[GrpcReadJournal])

  /**
   * Construct a gRPC read journal for the given stream-id and explicit `GrpcClientSettings` to control
   * how to reach the Akka Projection gRPC producer service (host, port etc).
   */
  def apply(
      system: ClassicActorSystemProvider,
      streamId: String,
      clientSettings: GrpcClientSettings): GrpcReadJournal = {
    val extended = system.classicSystem.asInstanceOf[ExtendedActorSystem]

    val configPath = GrpcReadJournal.Identifier
    val config = ConfigFactory
      .parseString(s"""akka.projection.grpc.consumer.stream-id=$streamId""")
      .withFallback(extended.settings.config)
      .getConfig(configPath)

    val settings = GrpcQuerySettings(config)

    val clientSettingsWithOverrides =
      // compose with potential user overrides to allow overriding our defaults
      clientSettings.withChannelBuilderOverrides(
        channelBuilderOverrides.andThen(clientSettings.channelBuilderOverrides))

    new scaladsl.GrpcReadJournal(extended, settings, clientSettingsWithOverrides)
  }

  private def channelBuilderOverrides: NettyChannelBuilder => NettyChannelBuilder =
    _.keepAliveWithoutCalls(true)
      .keepAliveTime(10, TimeUnit.SECONDS)
      .keepAliveTimeout(5, TimeUnit.SECONDS)

}

final class GrpcReadJournal private (
    system: ExtendedActorSystem,
    settings: GrpcQuerySettings,
    clientSettings: GrpcClientSettings)
    extends ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery {
  import GrpcReadJournal.log

  private def this(system: ExtendedActorSystem, settings: GrpcQuerySettings) =
    this(
      system,
      settings,
      GrpcClientSettings
        .fromConfig(settings.grpcClientConfig)(system)
        .withChannelBuilderOverrides(GrpcReadJournal.channelBuilderOverrides))

  // entry point when created through Akka Persistence
  def this(system: ExtendedActorSystem, config: Config, cfgPath: String) =
    this(system, GrpcQuerySettings(config))

  private implicit val typedSystem = system.toTyped
  private val persistenceExt = Persistence(system)
  private val protoAnySerialization =
    new ProtoAnySerialization(system.toTyped, settings.protoClassMapping)

  private val client = EventProducerServiceClient(clientSettings)

  override def sliceForPersistenceId(persistenceId: String): Int =
    persistenceExt.sliceForPersistenceId(persistenceId)

  override def sliceRanges(numberOfRanges: Int): immutable.Seq[Range] =
    persistenceExt.sliceRanges(numberOfRanges)

  /**
   * Query events for given slices. A slice is deterministically defined based on the persistence id. The purpose is to
   * evenly distribute all persistence ids over the slices.
   *
   * The consumer can keep track of its current position in the event stream by storing the `offset` and restart the
   * query from a given `offset` after a crash/restart.
   *
   * The supported offset is [[TimestampOffset]] and [[Offset.noOffset]].
   *
   * The timestamp is based on the database `transaction_timestamp()` when the event was stored.
   * `transaction_timestamp()` is the time when the transaction started, not when it was committed. This means that a
   * "later" event may be visible first and when retrieving events after the previously seen timestamp we may miss some
   * events. In distributed SQL databases there can also be clock skews for the database timestamps. For that reason it
   * will perform additional backtracking queries to catch missed events. Events from backtracking will typically be
   * duplicates of previously emitted events. It's the responsibility of the consumer to filter duplicates and make sure
   * that events are processed in exact sequence number order for each persistence id. Such deduplication is provided by
   * the R2DBC Projection.
   *
   * Events emitted by the backtracking don't contain the event payload (`EventBySliceEnvelope.event` is None) and the
   * consumer can load the full `EventBySliceEnvelope` with [[GrpcReadJournal.loadEnvelope]].
   *
   * The events will be emitted in the timestamp order with the caveat of duplicate events as described above. Events
   * with the same timestamp are ordered by sequence number.
   *
   * The stream is not completed when it reaches the end of the currently stored events, but it continues to push new
   * events when new events are persisted.
   */
  override def eventsBySlices[Evt](
      // note that this is actually the producer side defined stream_id
      // not the normal entity type which is internal to the producing side
      streamId: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Evt], NotUsed] = {
    require(streamId == settings.streamId, s"Stream id mismatch, was [$streamId], expected [${settings.streamId}]")
    log.debug(
      "Starting eventsBySlices stream from [{}] [{}], slices [{} - {}], offset [{}]",
      clientSettings.serviceName,
      streamId,
      minSlice,
      maxSlice,
      offset match {
        case t: TimestampOffset => t.timestamp
        case _                  => offset
      })

    val protoOffset =
      offset match {
        case o: TimestampOffset =>
          val protoTimestamp = Timestamp(o.timestamp)
          val protoSeen = o.seen.iterator.map {
            case (pid, seqNr) =>
              PersistenceIdSeqNr(pid, seqNr)
          }.toSeq
          Some(proto.Offset(Some(protoTimestamp), protoSeen))
        case NoOffset =>
          None
        case _ =>
          throw new IllegalArgumentException(s"Expected TimestampOffset or NoOffset, but got [$offset]")
      }

    val initReq = InitReq(streamId, minSlice, maxSlice, protoOffset)
    val streamIn = Source
      .single(StreamIn(StreamIn.Message.Init(initReq)))
      .concat(Source.maybe)
    val streamOut =
      client.eventsBySlices(streamIn).recover {
        case th: Throwable =>
          throw new RuntimeException(s"Failure to consume gRPC event stream for [${streamId}]", th)
      }

    streamOut.map {
      case StreamOut(StreamOut.Message.Event(event), _) =>
        if (log.isTraceEnabled)
          log.trace(
            "Received {}event from [{}] persistenceId [{}] with seqNr [{}], offset [{}]",
            if (event.payload.isEmpty) "backtracking " else "",
            clientSettings.serviceName,
            event.persistenceId,
            event.seqNr,
            timestampOffset(event.offset.get).timestamp)

        eventToEnvelope(event, streamId)

      case StreamOut(StreamOut.Message.FilteredEvent(filteredEvent), _) =>
        if (log.isTraceEnabled)
          log.trace(
            "Received filtered event from [{}] persistenceId [{}] with seqNr [{}], offset [{}]",
            clientSettings.serviceName,
            filteredEvent.persistenceId,
            filteredEvent.seqNr,
            timestampOffset(filteredEvent.offset.get).timestamp)

        filteredEventToEnvelope(filteredEvent, streamId)

      case other =>
        throw new IllegalArgumentException(s"Unexpected StreamOut [${other.message.getClass.getName}]")
    }
  }

  private def eventToEnvelope[Evt](
      event: Event,
      // note that this is actually the producer side defined stream_id
      // not the normal entity type which is internal to the producing side
      streamId: String): EventEnvelope[Evt] = {
    require(streamId == settings.streamId, s"Stream id mismatch, was [$streamId], expected [${settings.streamId}]")
    val eventOffset = timestampOffset(event.offset.get)
    val evt =
      event.payload.map(protoAnySerialization.decode(_).asInstanceOf[Evt])

    new EventEnvelope(
      eventOffset,
      event.persistenceId,
      event.seqNr,
      evt,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = None,
      PersistenceId.extractEntityType(event.persistenceId),
      event.slice)
  }

  private def filteredEventToEnvelope[Evt](filteredEvent: FilteredEvent, entityType: String): EventEnvelope[Evt] = {
    val eventOffset = timestampOffset(filteredEvent.offset.get)

    // Note that envelope is marked with NotUsed in the eventMetadata. That is handled by the R2dbcProjection
    // implementation to skip the envelope and still store the offset.
    new EventEnvelope(
      eventOffset,
      filteredEvent.persistenceId,
      filteredEvent.seqNr,
      None,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = Some(NotUsed),
      entityType,
      filteredEvent.slice)
  }

  private def timestampOffset(protoOffset: akka.projection.grpc.internal.proto.Offset): TimestampOffset = {
    val timestamp = protoOffset.timestamp.get.asJavaInstant
    val seen = protoOffset.seen.map {
      case PersistenceIdSeqNr(pid, seqNr, _) =>
        pid -> seqNr
    }.toMap
    TimestampOffset(timestamp, seen)
  }

  // EventTimestampQuery
  override def timestampOf(persistenceId: String, sequenceNr: Long): Future[Option[Instant]] = {
    import system.dispatcher
    client
      .eventTimestamp(EventTimestampRequest(settings.streamId, persistenceId, sequenceNr))
      .map(_.timestamp.map(_.asJavaInstant))
  }

  //LoadEventQuery
  override def loadEnvelope[Evt](persistenceId: String, sequenceNr: Long): Future[EventEnvelope[Evt]] = {
    log.trace(
      "Loading event from [{}] persistenceId [{}] with seqNr [{}]",
      clientSettings.serviceName,
      persistenceId,
      sequenceNr)
    import system.dispatcher
    client
      .loadEvent(LoadEventRequest(settings.streamId, persistenceId, sequenceNr))
      .map {
        case LoadEventResponse(LoadEventResponse.Message.Event(event), _) =>
          eventToEnvelope(event, settings.streamId)

        case LoadEventResponse(LoadEventResponse.Message.FilteredEvent(filteredEvent), _) =>
          filteredEventToEnvelope(filteredEvent, settings.streamId)

        case other =>
          throw new IllegalArgumentException(s"Unexpected LoadEventResponse [${other.message.getClass.getName}]")

      }
  }

}
