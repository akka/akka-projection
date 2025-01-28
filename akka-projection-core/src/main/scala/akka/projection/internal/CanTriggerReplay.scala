/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] trait CanTriggerReplay {
  private[akka] def triggerReplay(persistenceId: String, fromSeqNr: Long, triggeredBySeqNr: Long): Unit
}
