/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.sql.Connection
import java.sql.Timestamp
import java.time.Clock
import java.time.Instant

import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.internal.OffsetSerialization.fromStorageRepresentation
import akka.projection.internal.OffsetSerialization.toStorageRepresentation
import akka.projection.jdbc.internal.DialectDefaults.InsertIndices
import akka.projection.jdbc.javadsl.JdbcSession
import akka.projection.jdbc.javadsl.JdbcSession.tryWithResource
import akka.projection.jdbc.javadsl.JdbcSession.withConnection

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class JdbcOffsetStore[S <: JdbcSession](
    settings: JdbcSettings,
    jdbcSessionFactory: () => S,
    clock: Clock) {

  def this(settings: JdbcSettings, jdbcSessionFactory: () => S) =
    this(settings, jdbcSessionFactory, Clock.systemUTC())

  private[akka] implicit val executionContext = settings.executionContext

  def createIfNotExists(): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      tryWithResource(conn.createStatement()) { stmt =>
        stmt.execute(settings.dialect.createTableStatement)
        stmt.execute(settings.dialect.alterTableStatement)
        Done
      }
    }
  }

  def clearOffset(projectionId: ProjectionId): Future[Done] =
    withConnection(jdbcSessionFactory) { conn =>
      tryWithResource(conn.prepareStatement(settings.dialect.clearOffsetStatement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)
        stmt.executeUpdate()
        Done
      }
    }

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] =
    withConnection(jdbcSessionFactory) { conn =>
      // init Statement in try-with-resource
      tryWithResource(conn.prepareStatement(settings.dialect.readOffsetQuery)) { stmt =>

        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)

        // init ResultSet in try-with-resource
        tryWithResource(stmt.executeQuery()) { resultSet =>
          // FIXME: need to iterate over all results to build MultipleOffsets
          if (resultSet.first()) {
            val offsetStr = resultSet.getString("OFFSET")
            val manifest = resultSet.getString("MANIFEST")
            Some(fromStorageRepresentation(offsetStr, manifest))
          } else None
        }
      }
    }

  /**
   * Like saveOffset, but async. Useful for resetting an offset
   */
  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] =
    withConnection(jdbcSessionFactory) { conn =>
      saveOffsetBlocking(conn, projectionId, offset)
    }

  /**
   * This method is explicitly made non-async because it's used together
   * with the users' handler code and run inside the same Future
   */
  def saveOffsetBlocking[Offset](conn: Connection, projectionId: ProjectionId, offset: Offset): Done = {

    tryWithResource(conn.prepareStatement(settings.dialect.insertOrUpdateStatement)) { stmt =>

      toStorageRepresentation(projectionId, offset) match {
        case SingleOffset(id, manifest, offsetStr, mergeable) =>
          stmt.setString(InsertIndices.PROJECTION_NAME, id.name)
          stmt.setString(InsertIndices.PROJECTION_KEY, id.key)
          stmt.setString(InsertIndices.OFFSET, offsetStr)
          stmt.setString(InsertIndices.MANIFEST, manifest)
          stmt.setBoolean(InsertIndices.MERGEABLE, mergeable)
          val now: Instant = Instant.now(clock)
          stmt.setTimestamp(InsertIndices.LAST_UPDATED, new Timestamp(now.toEpochMilli))

        case _ =>
          // FIXME: add support for MultipleOffsets
          throw new IllegalArgumentException("The JdbcOffsetStore does not currently support MergeableOffset")
      }

      stmt.executeUpdate()
      Done
    }
  }

}
