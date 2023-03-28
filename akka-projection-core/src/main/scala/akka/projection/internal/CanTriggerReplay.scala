/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 */
@InternalApi
trait CanTriggerReplay {
  def triggerReplay(envelope: EventEnvelope[Any]): Unit
}
