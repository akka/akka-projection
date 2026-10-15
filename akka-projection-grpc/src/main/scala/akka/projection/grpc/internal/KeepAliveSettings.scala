/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration._

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KeepAliveSettings {

  // bounds for consumer requested keep alive interval, enforced on both sides
  val MinInterval: FiniteDuration = 1.second
  val MaxInterval: FiniteDuration = 1.hour

  def validate(interval: Option[FiniteDuration], timeout: Option[FiniteDuration]): Unit = {
    interval.foreach { i =>
      require(
        i >= MinInterval && i <= MaxInterval,
        s"keepAliveInterval must be between [$MinInterval] and [$MaxInterval], was [$i]")
    }
    timeout.foreach { t =>
      require(interval.isDefined, "keepAliveTimeout requires keepAliveInterval")
      val i = interval.get
      require(t >= i * 2, s"keepAliveTimeout must be at least twice the keepAliveInterval [$i], was [$t]")
    }
  }

  // producer side, bounds the consumer requested interval, compares raw millis since huge values overflow FiniteDuration
  def clampInterval(requestedMillis: Long): FiniteDuration =
    if (requestedMillis >= MaxInterval.toMillis) MaxInterval
    else requestedMillis.millis.max(MinInterval)

  // producer side, the limit for keep alives from the consumer, none when the consumer did not ask for a timeout
  def producerTimeout(requestedIntervalMillis: Long, requestedTimeoutMillis: Long): Option[FiniteDuration] =
    if (requestedTimeoutMillis <= 0) None
    else {
      val minTimeout = clampInterval(requestedIntervalMillis) * 2
      val maxTimeout = MaxInterval * 2
      if (requestedTimeoutMillis >= maxTimeout.toMillis) Some(maxTimeout)
      else Some(requestedTimeoutMillis.millis.max(minTimeout))
    }
}
