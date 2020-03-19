/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import akka.persistence.query.Offset

// same as akka.persistence.query.EventEnvelope but with typed Event
final case class EventEnvelope[Event](
    offset: Offset,
    persistenceId: String,
    sequenceNr: Long,
    event: Event,
    timestamp: Long)
