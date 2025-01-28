/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object OffsetPidSeqNr {
  def apply(offset: Any, pid: String, seqNr: Long): OffsetPidSeqNr =
    new OffsetPidSeqNr(offset, Some(pid -> seqNr))

  def apply(offset: Any): OffsetPidSeqNr =
    new OffsetPidSeqNr(offset, None)
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] final case class OffsetPidSeqNr(offset: Any, pidSeqNr: Option[(String, Long)])
