/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.projection.internal.CorrelationId

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class KeepAliveTimeoutException(
    streamId: String,
    timeout: FiniteDuration,
    correlationId: Option[String],
    cause: Throwable)
    extends RuntimeException(
      s"No events or keep alive messages received within [${timeout.toCoarsest}] for gRPC event stream " +
      s"[$streamId]${CorrelationId.toLogText(correlationId)}, connection may be stale, the producer may not " +
      "support keep alives, or the projection handler took longer than the timeout, will be retried",
      cause)
