/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionId
import akka.projection.r2dbc.R2dbcProjectionSettings

/**
 * INTERNAL API
 */
@InternalApi
private[projection] final class H2OffsetStoreDao(
    settings: R2dbcProjectionSettings,
    sourceProvider: Option[BySlicesSourceProvider],
    system: ActorSystem[_],
    executor: R2dbcExecutor,
    projectionId: ProjectionId)
    extends PostgresOffsetStoreDao(settings, sourceProvider, system, executor, projectionId) {
  override protected def createUpsertOffsetSql(): String =
    sql"""
      MERGE INTO ${settings.offsetTable}
      (projection_name, projection_key, current_offset, manifest, mergeable, last_updated)
      KEY(projection_name, projection_key)
      VALUES (?,?,?,?,?,?)
     """

  override protected def createUpdateManagementStateSql(): String =
    sql"""
     MERGE INTO ${settings.managementTable}
     (projection_name, projection_key, paused, last_updated)
     KEY(projection_name, projection_key)
     VALUES (?,?,?,?)
  """

}
