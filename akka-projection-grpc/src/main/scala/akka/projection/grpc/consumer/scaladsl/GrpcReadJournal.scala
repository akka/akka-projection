/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.consumer.scaladsl

import java.time.Instant
import java.util.concurrent.TimeUnit

import scala.collection.immutable
import scala.concurrent.Future

import akka.NotUsed
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
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.projection.grpc.internal.proto
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventProducerServiceClient
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.stream.scaladsl.Source
import com.google.protobuf.timestamp.Timestamp
import com.typesafe.config.Config

object GrpcReadJournal {
  val Identifier = "akka.projection.grpc.consumer"
}

final class GrpcReadJournal(
    system: ExtendedActorSystem,
    config: Config,
    cfgPath: String)
    extends ReadJournal
    with EventsBySliceQuery
    with EventTimestampQuery
    with LoadEventQuery {

  private val settings = GrpcQuerySettings(
    system.settings.config.getConfig(cfgPath))

  private implicit val typedSystem = system.toTyped
  private val persistenceExt = Persistence(system)
  private val protoAnySerialization =
    new ProtoAnySerialization(system.toTyped, settings.protoClassMapping)

  private val clientSettings =
    GrpcClientSettings
      .fromConfig(settings.grpcClientConfig)
      .withChannelBuilderOverrides(
        _.keepAliveWithoutCalls(true)
          .keepAliveTime(10, TimeUnit.SECONDS)
          .keepAliveTimeout(5, TimeUnit.SECONDS))
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
      entityType: String,
      minSlice: Int,
      maxSlice: Int,
      offset: Offset): Source[EventEnvelope[Evt], NotUsed] = {

    val protoOffset =
      offset match {
        case o: TimestampOffset =>
          val protoTimestamp = Timestamp(o.timestamp)
          val protoSeen = o.seen.iterator.map { case (pid, seqNr) =>
            PersistenceIdSeqNr(pid, seqNr)
          }.toSeq
          Some(proto.Offset(Some(protoTimestamp), protoSeen))
        case NoOffset =>
          None
        case _ =>
          throw new IllegalArgumentException(
            s"Expected TimestampOffset or NoOffset, but got [$offset]")
      }

    val initReq = InitReq(entityType, minSlice, maxSlice, protoOffset)
    val streamIn = Source
      .single(StreamIn(StreamIn.Message.Init(initReq)))
      .concat(Source.maybe)
    val streamOut = client.eventsBySlices(streamIn)
    streamOut.map {
      case StreamOut(
            StreamOut.Message.Event(
              Event(
                persistenceId,
                seqNr,
                slice,
                Some(protoEventOffset),
                protoEvent,
                _)),
            _) =>
        val timestamp = protoEventOffset.timestamp.get.asJavaInstant
        val seen = protoEventOffset.seen.map {
          case PersistenceIdSeqNr(pid, seqNr, _) => pid -> seqNr
        }.toMap
        val eventOffset = TimestampOffset(timestamp, seen)

        val event =
          protoEvent.map(protoAnySerialization.decode(_).asInstanceOf[Evt])

        new EventEnvelope(
          eventOffset,
          persistenceId,
          seqNr,
          event,
          timestamp.toEpochMilli,
          eventMetadata = None,
          entityType,
          slice)

      case StreamOut(
            StreamOut.Message.FilteredEvent(
              FilteredEvent(
                persistenceId,
                seqNr,
                slice,
                Some(protoEventOffset),
                _)),
            _) =>
        val timestamp = protoEventOffset.timestamp.get.asJavaInstant
        val seen = protoEventOffset.seen.map {
          case PersistenceIdSeqNr(pid, seqNr, _) => pid -> seqNr
        }.toMap
        val eventOffset = TimestampOffset(timestamp, seen)

        // Note that envelope is marked with NotUsed in the eventMetadata. That is handled by the R2dbcProjection
        // implementation to skip the envelope and still store the offset.
        new EventEnvelope(
          eventOffset,
          persistenceId,
          seqNr,
          None,
          timestamp.toEpochMilli,
          eventMetadata = Some(NotUsed),
          entityType,
          slice)

      case other =>
        throw new IllegalArgumentException(
          s"Unexpected StreamOut [${other.message.getClass.getName}]")
    }
  }

  // EventTimestampQuery
  override def timestampOf(
      persistenceId: String,
      sequenceNr: Long): Future[Option[Instant]] = {
    ??? //FIXME do we need this?
  }

  //LoadEventQuery
  override def loadEnvelope[Evt](
      persistenceId: String,
      sequenceNr: Long): Future[EventEnvelope[Evt]] = {
    ??? //FIXME do we need this?
  }

}
