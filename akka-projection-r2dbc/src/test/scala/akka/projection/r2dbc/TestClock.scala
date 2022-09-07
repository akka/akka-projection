/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi private[projection] class TestClock extends Clock {

  @volatile private var _instant = roundToMillis(Instant.now())

  override def getZone: ZoneId = ZoneOffset.UTC

  override def withZone(zone: ZoneId): Clock =
    throw new UnsupportedOperationException("withZone not supported")

  override def instant(): Instant =
    _instant

  def setInstant(newInstant: Instant): Unit =
    _instant = roundToMillis(newInstant)

  def tick(duration: Duration): Instant = {
    val newInstant = roundToMillis(_instant.plus(duration))
    _instant = newInstant
    newInstant
  }

  private def roundToMillis(i: Instant): Instant = {
    // algo taken from java.time.Clock.tick
    val epochMilli = i.toEpochMilli
    Instant.ofEpochMilli(epochMilli - Math.floorMod(epochMilli, 1L))
  }

}
