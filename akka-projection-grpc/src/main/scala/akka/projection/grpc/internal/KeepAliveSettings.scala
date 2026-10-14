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

  // lower bound for consumer requested keep alive interval, enforced on both sides
  val MinInterval: FiniteDuration = 1.second

  def validate(interval: Option[FiniteDuration], timeout: Option[FiniteDuration]): Unit = {
    require(
      interval.forall(_ >= MinInterval),
      s"keepAliveInterval must be at least [${MinInterval.toCoarsest}], was [${interval.fold("")(_.toCoarsest.toString)}]")
    require(
      timeout.forall(t => interval.exists(_ < t)),
      "keepAliveTimeout must be larger than keepAliveInterval and can only be used together with keepAliveInterval")
  }
}
