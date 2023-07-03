/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success

/**
 * INTERNAL API
 *
 * Common conversions between protobuf protocols and Akka APIs
 */
private[akka] object ProtobufProtocolConversions {

  def protocolOffsetToOffset(offset: Option[proto.Offset]): Offset =
    offset match {
      case None => NoOffset
      case Some(o) =>
        val timestamp =
          o.timestamp.map(_.asJavaInstant).getOrElse(Instant.EPOCH)
        val seen = o.seen.map {
          case PersistenceIdSeqNr(pid, seqNr, _) =>
            pid -> seqNr
        }.toMap
        TimestampOffset(timestamp, seen)
    }

  def offsetToProtoOffset(offset: Offset): Option[proto.Offset] = {
    offset match {
      case TimestampOffset(timestamp, _, seen) =>
        val protoTimestamp = Timestamp(timestamp)
        val protoSeen = seen.iterator.map {
          case (pid, seqNr) =>
            PersistenceIdSeqNr(pid, seqNr)
        }.toSeq
        Some(proto.Offset(Some(protoTimestamp), protoSeen))
      case NoOffset => None
      case other =>
        throw new IllegalArgumentException(s"Unexpected offset type [$other]")
    }
  }

  def transformAndEncodeEvent(
      transformation: Transformation,
      env: EventEnvelope[_],
      protoAnySerialization: ProtoAnySerialization)(
      implicit executionContext: ExecutionContext): Future[Option[Event]] = {
    env.eventOption match {
      case Some(_) =>
        val mappedFuture: Future[Option[Any]] = transformation(env.asInstanceOf[EventEnvelope[Any]])

        def toEvent(transformedEvent: Any): Event = {
          val protoEvent = protoAnySerialization.serialize(transformedEvent)
          val metadata = env.eventMetadata.map(protoAnySerialization.serialize)
          Event(
            persistenceId = env.persistenceId,
            seqNr = env.sequenceNr,
            slice = env.slice,
            offset = ProtobufProtocolConversions.offsetToProtoOffset(env.offset),
            payload = Some(protoEvent),
            metadata = metadata,
            source = env.source,
            tags = env.tags.toSeq)
        }

        mappedFuture.value match {
          case Some(Success(Some(transformedEvent))) => Future.successful(Some(toEvent(transformedEvent)))
          case Some(Success(None))                   => Future.successful(None)
          case _                                     => mappedFuture.map(_.map(toEvent))
        }

      case None =>
        // Events from backtracking are lazily loaded via `loadEvent` if needed.
        // Transformation and filter is done via `loadEvent` in that case.
        Future.successful(
          Some(
            Event(
              persistenceId = env.persistenceId,
              seqNr = env.sequenceNr,
              slice = env.slice,
              offset = ProtobufProtocolConversions.offsetToProtoOffset(env.offset),
              payload = None,
              source = env.source,
              tags = env.tags.toSeq)))
    }
  }

  def eventToEnvelope[Evt](event: Event, protoAnySerialization: ProtoAnySerialization): EventEnvelope[Evt] = {
    val eventOffset = protocolOffsetToOffset(event.offset).asInstanceOf[TimestampOffset]
    val evt =
      event.payload.map(protoAnySerialization.deserialize(_).asInstanceOf[Evt])

    val metadata: Option[Any] = event.metadata.map(protoAnySerialization.deserialize)

    new EventEnvelope(
      eventOffset,
      event.persistenceId,
      event.seqNr,
      evt,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = metadata,
      PersistenceId.extractEntityType(event.persistenceId),
      event.slice,
      filtered = false,
      source = event.source,
      tags = event.tags.toSet)
  }

  def filteredEventToEnvelope[Evt](filtered: FilteredEvent): EventEnvelope[Evt] = {
    val eventOffset = protocolOffsetToOffset(filtered.offset).asInstanceOf[TimestampOffset]

    new EventEnvelope(
      eventOffset,
      filtered.persistenceId,
      filtered.seqNr,
      None,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = None,
      PersistenceId.extractEntityType(filtered.persistenceId),
      filtered.slice,
      filtered = false,
      source = filtered.source,
      tags = Set.empty)
  }

}
