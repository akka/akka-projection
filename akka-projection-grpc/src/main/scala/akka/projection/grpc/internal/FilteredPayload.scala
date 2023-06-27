/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.BySlicesSourceProvider
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

import scala.concurrent.Future

/**
 * INTERNAL API
 *
 * Placeholder object for filtered events when producer event push is used
 */
@InternalApi
private[akka] case object FilteredPayload

/**
 * INTERNAL API
 *
 * Turns envelopes with placeholder events into filtered envelopes on the consuming side of the journal
 */
private[akka] final class FilteredPayloadMapper[OffsetType, Event](
    actual: SourceProvider[OffsetType, EventEnvelope[Event]])
    extends SourceProvider[OffsetType, EventEnvelope[Event]]
    with BySlicesSourceProvider {
  override def source(offset: () => Future[Option[OffsetType]]): Future[Source[EventEnvelope[Event], NotUsed]] =
    actual
      .source(offset)
      .map(source =>
        source.map { envelope =>
          envelope.eventOption.asInstanceOf[Option[Any]] match {
            case Some(FilteredPayload) =>
              new EventEnvelope[Event](
                persistenceId = envelope.persistenceId,
                offset = envelope.offset,
                entityType = envelope.entityType,
                sequenceNr = envelope.sequenceNr,
                eventOption = None,
                timestamp = envelope.timestamp,
                eventMetadata = None,
                slice = envelope.slice,
                filtered = true,
                source = envelope.source,
                tags = envelope.tags)
            case _ => envelope
          }
        })(ExecutionContexts.parasitic)

  override def extractOffset(envelope: EventEnvelope[Event]): OffsetType = actual.extractOffset(envelope)

  override def extractCreationTime(envelope: EventEnvelope[Event]): Long = actual.extractCreationTime(envelope)

  override def minSlice: Int = actual.asInstanceOf[BySlicesSourceProvider].minSlice
  override def maxSlice: Int = actual.asInstanceOf[BySlicesSourceProvider].maxSlice
}
