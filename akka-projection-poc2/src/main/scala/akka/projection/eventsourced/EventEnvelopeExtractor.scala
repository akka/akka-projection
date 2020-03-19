/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import akka.persistence.query.Offset
import akka.projection.scaladsl.EnvelopeExtractor

class EventEnvelopeExtractor[Event]
    extends EnvelopeExtractor[akka.persistence.query.EventEnvelope, EventEnvelope[Event], Offset] {

  override def extractOffset(envelope: akka.persistence.query.EventEnvelope): Offset =
    envelope.offset

  override def extractPayload(envelope: akka.persistence.query.EventEnvelope): EventEnvelope[Event] =
    EventEnvelope(
      envelope.offset,
      envelope.persistenceId,
      envelope.sequenceNr,
      envelope.event.asInstanceOf[Event],
      envelope.timestamp)

}
