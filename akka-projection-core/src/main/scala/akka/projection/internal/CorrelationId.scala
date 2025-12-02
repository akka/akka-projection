/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object CorrelationId {
  def toLogText(correlationid: Option[String]) = correlationid match {
    case Some(id) => s", correlation [$id]"
    case None     => ""
  }

  def toCorrelationLogText(correlationid: String) =
    if (correlationid.isEmpty) correlationid
    else s", correlation [$correlationid]"

}
