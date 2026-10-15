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
    correlationId: Option[String])
    extends RuntimeException(
      s"No keep alive or event received from producer within [${timeout.toCoarsest}] while waiting for events on " +
      s"gRPC event stream [$streamId]${CorrelationId.toLogText(correlationId)}, connection may be stale, will be retried")
