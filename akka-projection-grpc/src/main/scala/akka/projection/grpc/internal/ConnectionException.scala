/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.annotation.InternalApi

import scala.util.control.NoStackTrace

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ConnectionException(host: String, port: String, streamId: String)
    extends RuntimeException(s"Connection to $host:$port for stream id $streamId failed or lost")
    with NoStackTrace
