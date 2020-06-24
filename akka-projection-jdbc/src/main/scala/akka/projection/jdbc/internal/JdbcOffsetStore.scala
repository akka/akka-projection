/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.sql.Connection
import java.sql.Timestamp
import java.time.Clock
import java.time.Instant

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.DialectDefaults.InsertIndices
import akka.projection.jdbc.internal.JdbcSessionUtil._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class JdbcOffsetStore[S <: JdbcSession](
    system: ActorSystem[_],
    settings: JdbcSettings,
    jdbcSessionFactory: () => S,
    clock: Clock) {

  def this(system: ActorSystem[_], settings: JdbcSettings, jdbcSessionFactory: () => S) =
    this(system, settings, jdbcSessionFactory, Clock.systemUTC())

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

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

        // init ResultSet in try-with-resource
        tryWithResource(stmt.executeQuery()) { resultSet =>

          val buffer = ListBuffer.empty[SingleOffset]

          while (resultSet.next()) {
            val offsetStr = resultSet.getString("OFFSET")
            val manifest = resultSet.getString("MANIFEST")
            val mergeable = resultSet.getBoolean("MERGEABLE")
            val key = resultSet.getString("PROJECTION_KEY")

            val adaptedProjectionId = ProjectionId(projectionId.name, key)
            val single = SingleOffset(adaptedProjectionId, manifest, offsetStr, mergeable)
            buffer.append(single)
          }

          if (buffer.isEmpty) None
          else if (buffer.forall(_.mergeable)) {
            Some(
              fromStorageRepresentation[MergeableOffset[_, _], Offset](MultipleOffsets(buffer.toList))
                .asInstanceOf[Offset])
          } else {
            buffer.find(_.id == projectionId).map(fromStorageRepresentation[Offset, Offset])
          }
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

      val now = new Timestamp(Instant.now(clock).toEpochMilli)

      def newRow(singleOffset: SingleOffset) = {
        stmt.setString(InsertIndices.PROJECTION_NAME, singleOffset.id.name)
        stmt.setString(InsertIndices.PROJECTION_KEY, singleOffset.id.key)
        stmt.setString(InsertIndices.OFFSET, singleOffset.offsetStr)
        stmt.setString(InsertIndices.MANIFEST, singleOffset.manifest)
        stmt.setBoolean(InsertIndices.MERGEABLE, singleOffset.mergeable)
        stmt.setTimestamp(InsertIndices.LAST_UPDATED, now)
        stmt.addBatch()
      }

      toStorageRepresentation(projectionId, offset) match {
        case single: SingleOffset  => newRow(single)
        case MultipleOffsets(many) => many.foreach(newRow)
      }

      stmt.executeBatch()

      Done
    }
  }

}
