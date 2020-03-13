/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import akka.persistence.query.EventEnvelope
import akka.persistence.query.Offset
import akka.projection.scaladsl.EnvelopeExtractor

class EventEnvelopeExtractor[Event] extends EnvelopeExtractor[EventEnvelope, Event, Offset] {
  override def extractOffset(envelope: EventEnvelope): Offset = envelope.offset

  override def extractPayload(envelope: EventEnvelope): Event = envelope.event.asInstanceOf[Event]
}
