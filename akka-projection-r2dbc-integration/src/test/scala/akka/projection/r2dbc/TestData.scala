/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.projection.ProjectionId

object TestData {
  private val start = 0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeCounter

  def nextPid() = s"p-${pidCounter.incrementAndGet()}"
  // FIXME return PersistenceId instead
  def nextPid(entityType: String) = s"$entityType|p-${pidCounter.incrementAndGet()}"

  def nextEntityType() = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

  def genRandomProjectionId(): ProjectionId = ProjectionId(UUID.randomUUID().toString, "00")

}
