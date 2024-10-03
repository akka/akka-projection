/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.annotation.InternalApi
import akka.persistence.query.NoOffset
import akka.persistence.query.Offset
import akka.persistence.query.TimestampOffset
import akka.persistence.query.TimestampOffsetBySlice
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.internal.FilterStage.Filter
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.ExcludeEntityIds
import akka.projection.grpc.internal.proto.ExcludeRegexEntityIds
import akka.projection.grpc.internal.proto.ExcludeTags
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.IncludeEntityIds
import akka.projection.grpc.internal.proto.IncludeRegexEntityIds
import akka.projection.grpc.internal.proto.IncludeTags
import akka.projection.grpc.internal.proto.IncludeTopics
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.RemoveExcludeEntityIds
import akka.projection.grpc.internal.proto.RemoveExcludeRegexEntityIds
import akka.projection.grpc.internal.proto.RemoveExcludeTags
import akka.projection.grpc.internal.proto.RemoveIncludeEntityIds
import akka.projection.grpc.internal.proto.RemoveIncludeRegexEntityIds
import akka.projection.grpc.internal.proto.RemoveIncludeTags
import akka.projection.grpc.internal.proto.RemoveIncludeTopics
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import com.google.protobuf.timestamp.Timestamp

import java.time.Instant
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success

/**
 * INTERNAL API
 *
 * Common conversions between protobuf protocols and Akka APIs
 */
@InternalApi
private[akka] object ProtobufProtocolConversions {

  def protocolOffsetToOffset(offsets: Seq[proto.Offset]): Offset =
    if (offsets.isEmpty) NoOffset
    else if (offsets.exists(_.slice.isDefined)) {
      val offsetBySlice = offsets.flatMap { offset =>
        offset.slice.map { _ -> protocolOffsetToTimestampOffset(offset) }
      }.toMap
      TimestampOffsetBySlice(offsetBySlice)
    } else {
      protocolOffsetToTimestampOffset(offsets.head)
    }

  private def protocolOffsetToTimestampOffset(offset: proto.Offset): TimestampOffset = {
    val timestamp = offset.timestamp match {
      case Some(ts) => ts.asJavaInstant
      case None     => Instant.EPOCH
    }
    // optimised for the expected normal case of one element
    val seen = if (offset.seen.size == 1) {
      Map(offset.seen.head.persistenceId -> offset.seen.head.seqNr)
    } else if (offset.seen.nonEmpty) {
      offset.seen.map {
        case PersistenceIdSeqNr(pid, seqNr, _) => pid -> seqNr
      }.toMap
    } else {
      Map.empty[String, Long]
    }
    TimestampOffset(timestamp, seen)
  }

  def offsetToProtoOffset(offset: Offset): Seq[proto.Offset] = {
    offset match {
      case timestampOffset: TimestampOffset =>
        Seq(timestampOffsetToProtoOffset(timestampOffset))
      case TimestampOffsetBySlice(offsets) =>
        offsets.iterator.map {
          case (slice, timestampOffset) =>
            timestampOffsetToProtoOffset(timestampOffset, Some(slice))
        }.toSeq
      case NoOffset => Seq.empty
      case other =>
        throw new IllegalArgumentException(s"Unexpected offset type [$other]")
    }
  }

  private def timestampOffsetToProtoOffset(offset: TimestampOffset, slice: Option[Int] = None): proto.Offset = {
    val protoTimestamp = Timestamp(offset.timestamp)
    val protoSeen = offset.seen.iterator.map {
      case (pid, seqNr) =>
        PersistenceIdSeqNr(pid, seqNr)
    }.toSeq
    proto.Offset(Some(protoTimestamp), protoSeen, slice)
  }

  def transformAndEncodeEvent(
      transformation: Transformation,
      env: EventEnvelope[_],
      protoAnySerialization: ProjectionGrpcSerialization)(
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

  def eventToEnvelope[Evt](event: Event, protoAnySerialization: ProjectionGrpcSerialization): EventEnvelope[Evt] =
    eventToEnvelope(event, protoAnySerialization, deserializeEvent = true).asInstanceOf[EventEnvelope[Evt]]

  def eventToEnvelope(
      event: Event,
      wireSerialization: ProjectionGrpcSerialization,
      deserializeEvent: Boolean): EventEnvelope[Any] = {
    val eventOffset = populateSeenIfNeeded(
      TimestampOffset.toTimestampOffset(protocolOffsetToOffset(event.offset)),
      event.persistenceId,
      event.seqNr)

    val metadata: Option[Any] = event.metadata.map(wireSerialization.deserialize)

    def envelopeWithDeserializedEvent: EventEnvelope[Any] = {
      val evt = event.payload.map(wireSerialization.deserialize)
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

    if (deserializeEvent || event.payload.isEmpty) {
      envelopeWithDeserializedEvent
    } else {
      wireSerialization.toSerializedEvent(event.payload.get) match {
        case Some(serializedEvent) =>
          new EventEnvelope(
            eventOffset,
            event.persistenceId,
            event.seqNr,
            eventOption = Some(serializedEvent),
            eventOffset.timestamp.toEpochMilli,
            eventMetadata = metadata,
            PersistenceId.extractEntityType(event.persistenceId),
            event.slice,
            filtered = false,
            source = event.source,
            tags = event.tags.toSet)
        case None =>
          // couldn't create SerializedEvent without deserialization, fallback to deserializeEvent = true
          envelopeWithDeserializedEvent
      }
    }
  }

  def filteredEventToEnvelope[Evt](filtered: FilteredEvent): EventEnvelope[Evt] = {
    val eventOffset = populateSeenIfNeeded(
      TimestampOffset.toTimestampOffset(protocolOffsetToOffset(filtered.offset)),
      filtered.persistenceId,
      filtered.seqNr)

    new EventEnvelope(
      eventOffset,
      filtered.persistenceId,
      filtered.seqNr,
      None,
      eventOffset.timestamp.toEpochMilli,
      eventMetadata = None,
      PersistenceId.extractEntityType(filtered.persistenceId),
      filtered.slice,
      filtered = true,
      source = filtered.source,
      tags = Set.empty)
  }

  private def populateSeenIfNeeded(offset: TimestampOffset, persistenceId: String, seqNr: Long): TimestampOffset = {
    if (offset.seen.isEmpty)
      TimestampOffset(offset.timestamp, offset.readTimestamp, seen = Map(persistenceId -> seqNr))
    else
      offset
  }

  def toProtoFilterCriteria(criteria: immutable.Seq[ConsumerFilter.FilterCriteria]): Seq[FilterCriteria] = {
    criteria.map {
      case ConsumerFilter.ExcludeTags(tags) =>
        FilterCriteria(FilterCriteria.Message.ExcludeTags(ExcludeTags(tags.toVector)))
      case ConsumerFilter.RemoveExcludeTags(tags) =>
        FilterCriteria(FilterCriteria.Message.RemoveExcludeTags(RemoveExcludeTags(tags.toVector)))
      case ConsumerFilter.IncludeTags(tags) =>
        FilterCriteria(FilterCriteria.Message.IncludeTags(IncludeTags(tags.toVector)))
      case ConsumerFilter.RemoveIncludeTags(tags) =>
        FilterCriteria(FilterCriteria.Message.RemoveIncludeTags(RemoveIncludeTags(tags.toVector)))
      case ConsumerFilter.IncludeTopics(expressions) =>
        FilterCriteria(FilterCriteria.Message.IncludeTopics(IncludeTopics(expressions.toVector)))
      case ConsumerFilter.RemoveIncludeTopics(expressions) =>
        FilterCriteria(FilterCriteria.Message.RemoveIncludeTopics(RemoveIncludeTopics(expressions.toVector)))
      case ConsumerFilter.IncludeEntityIds(entityOffsets) =>
        FilterCriteria(FilterCriteria.Message.IncludeEntityIds(IncludeEntityIds(entityOffsets.map {
          case ConsumerFilter.EntityIdOffset(entityId, seqNr) => EntityIdOffset(entityId, seqNr)
        }.toVector)))
      case ConsumerFilter.RemoveIncludeEntityIds(entityIds) =>
        FilterCriteria(FilterCriteria.Message.RemoveIncludeEntityIds(RemoveIncludeEntityIds(entityIds.toVector)))
      case ConsumerFilter.ExcludeEntityIds(entityIds) =>
        FilterCriteria(FilterCriteria.Message.ExcludeEntityIds(ExcludeEntityIds(entityIds.toVector)))
      case ConsumerFilter.RemoveExcludeEntityIds(entityIds) =>
        FilterCriteria(FilterCriteria.Message.RemoveExcludeEntityIds(RemoveExcludeEntityIds(entityIds.toVector)))
      case ConsumerFilter.ExcludeRegexEntityIds(matching) =>
        FilterCriteria(FilterCriteria.Message.ExcludeMatchingEntityIds(ExcludeRegexEntityIds(matching.toVector)))
      case ConsumerFilter.IncludeRegexEntityIds(matching) =>
        FilterCriteria(FilterCriteria.Message.IncludeMatchingEntityIds(IncludeRegexEntityIds(matching.toVector)))
      case ConsumerFilter.RemoveExcludeRegexEntityIds(matching) =>
        FilterCriteria(
          FilterCriteria.Message.RemoveExcludeMatchingEntityIds(RemoveExcludeRegexEntityIds(matching.toVector)))
      case ConsumerFilter.RemoveIncludeRegexEntityIds(matching) =>
        FilterCriteria(
          FilterCriteria.Message.RemoveIncludeMatchingEntityIds(RemoveIncludeRegexEntityIds(matching.toVector)))
    }
  }

  def updateFilterFromProto(
      initialFilter: Filter,
      criteria: Iterable[FilterCriteria],
      mapEntityIdToPidHandledByThisStream: Seq[String] => Seq[String]): Filter = {
    criteria.foldLeft(initialFilter) {
      case (acc, criteria) =>
        criteria.message match {
          case FilterCriteria.Message.IncludeTags(include) =>
            acc.addIncludeTags(include.tags)
          case FilterCriteria.Message.RemoveIncludeTags(include) =>
            acc.removeIncludeTags(include.tags)
          case FilterCriteria.Message.ExcludeTags(exclude) =>
            acc.addExcludeTags(exclude.tags)
          case FilterCriteria.Message.RemoveExcludeTags(exclude) =>
            acc.removeExcludeTags(exclude.tags)
          case FilterCriteria.Message.IncludeTopics(include) =>
            acc.addIncludeTopics(include.expression)
          case FilterCriteria.Message.RemoveIncludeTopics(include) =>
            acc.removeIncludeTopics(include.expression)
          case FilterCriteria.Message.IncludeEntityIds(include) =>
            val pids = mapEntityIdToPidHandledByThisStream(include.entityIdOffset.map(_.entityId))
            acc.addIncludePersistenceIds(pids)
          case FilterCriteria.Message.RemoveIncludeEntityIds(include) =>
            val pids = mapEntityIdToPidHandledByThisStream(include.entityIds)
            acc.removeIncludePersistenceIds(pids)
          case FilterCriteria.Message.ExcludeEntityIds(exclude) =>
            val pids = mapEntityIdToPidHandledByThisStream(exclude.entityIds)
            acc.addExcludePersistenceIds(pids)
          case FilterCriteria.Message.RemoveExcludeEntityIds(exclude) =>
            val pids = mapEntityIdToPidHandledByThisStream(exclude.entityIds)
            acc.removeExcludePersistenceIds(pids)
          case FilterCriteria.Message.ExcludeMatchingEntityIds(excludeRegex) =>
            acc.addExcludeRegexEntityIds(excludeRegex.matching)
          case FilterCriteria.Message.IncludeMatchingEntityIds(includeRegex) =>
            acc.addIncludeRegexEntityIds(includeRegex.matching)
          case FilterCriteria.Message.RemoveExcludeMatchingEntityIds(excludeRegex) =>
            acc.removeExcludeRegexEntityIds(excludeRegex.matching)
          case FilterCriteria.Message.RemoveIncludeMatchingEntityIds(includeRegex) =>
            acc.removeIncludeRegexEntityIds(includeRegex.matching)
          case FilterCriteria.Message.Empty =>
            acc
        }
    }
  }

}
