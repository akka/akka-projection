/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionId

object TestData {
  private val start =
    0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.pidCounter
  import TestData.entityTypeCounter

  def nextPid(entityType: String): PersistenceId =
    PersistenceId(entityType, s"p-${pidCounter.incrementAndGet()}")

  def nextEntityType() = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

  def randomProjectionId(): ProjectionId =
    ProjectionId(UUID.randomUUID().toString, "00")

}
