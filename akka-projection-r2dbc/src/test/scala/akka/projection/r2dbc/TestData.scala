/**
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.r2dbc

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.projection.ProjectionId

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeHintCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeHintCounter

  def nextPid() = s"p-${pidCounter.incrementAndGet()}"
  def nextPid(entityTypeHint: String) = s"$entityTypeHint|p-${pidCounter.incrementAndGet()}"

  def nextEntityTypeHint() = s"TestEntity-${entityTypeHintCounter.incrementAndGet()}"

  def genRandomProjectionId(): ProjectionId = ProjectionId(UUID.randomUUID().toString, "00")

}
