/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionId

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

object TestData {
  private val start =
    0L // could be something more unique, like currentTimeMillis
  private val pidCounter = new AtomicLong(start)
  private val entityTypeCounter = new AtomicLong(start)
}

trait TestData {
  import TestData.entityTypeCounter
  import TestData.pidCounter

  def nextPid(entityType: String): PersistenceId =
    PersistenceId(entityType, s"p-${pidCounter.incrementAndGet()}")

  def nextEntityType() = s"TestEntity-${entityTypeCounter.incrementAndGet()}"

  def randomProjectionId(): ProjectionId =
    ProjectionId(UUID.randomUUID().toString, "00")

}
