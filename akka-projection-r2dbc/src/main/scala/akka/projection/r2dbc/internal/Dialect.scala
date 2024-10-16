/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.r2dbc.R2dbcProjectionSettings

/**
 * INTERNAL API
 */
@InternalApi
private[projection] trait Dialect {
  def createOffsetStoreDao(
      settings: R2dbcProjectionSettings,
      sourceProvider: Option[BySlicesSourceProvider],
      system: ActorSystem[_],
      r2dbcExecutor: R2dbcExecutor,
      projectionId: ProjectionId): OffsetStoreDao
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object PostgresDialect extends Dialect {
  def createOffsetStoreDao(
      settings: R2dbcProjectionSettings,
      sourceProvider: Option[BySlicesSourceProvider],
      system: ActorSystem[_],
      r2dbcExecutor: R2dbcExecutor,
      projectionId: ProjectionId): OffsetStoreDao =
    new PostgresOffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object YugabyteDialect extends Dialect {
  def createOffsetStoreDao(
      settings: R2dbcProjectionSettings,
      sourceProvider: Option[BySlicesSourceProvider],
      system: ActorSystem[_],
      r2dbcExecutor: R2dbcExecutor,
      projectionId: ProjectionId): OffsetStoreDao =
    PostgresDialect.createOffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object H2Dialect extends Dialect {
  def createOffsetStoreDao(
      settings: R2dbcProjectionSettings,
      sourceProvider: Option[BySlicesSourceProvider],
      system: ActorSystem[_],
      r2dbcExecutor: R2dbcExecutor,
      projectionId: ProjectionId): OffsetStoreDao =
    new H2OffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId)
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object SqlServerDialect extends Dialect {
  def createOffsetStoreDao(
      settings: R2dbcProjectionSettings,
      sourceProvider: Option[BySlicesSourceProvider],
      system: ActorSystem[_],
      r2dbcExecutor: R2dbcExecutor,
      projectionId: ProjectionId): OffsetStoreDao =
    new SqlServerOffsetStoreDao(settings, sourceProvider, system, r2dbcExecutor, projectionId)
}
