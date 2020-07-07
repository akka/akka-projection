/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.sql.Connection
import java.sql.Statement
import java.sql.Timestamp
import java.time.Clock
import java.time.Instant

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.DialectDefaults.InsertIndices
import akka.projection.jdbc.internal.DialectDefaults.UpdateIndices
import akka.projection.jdbc.internal.JdbcSessionUtil._
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class JdbcOffsetStore[S <: JdbcSession](
    system: ActorSystem[_],
    settings: JdbcSettings,
    jdbcSessionFactory: () => S,
    clock: Clock) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val verboseLogging = logger.isDebugEnabled() && settings.verboseLoggingEnabled

  def this(system: ActorSystem[_], settings: JdbcSettings, jdbcSessionFactory: () => S) =
    this(system, settings, jdbcSessionFactory, Clock.systemUTC())

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  private[projection] implicit val executionContext = settings.executionContext

  def createIfNotExists(): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      logger.debug("creating offset-store table, using connection id [{}]", System.identityHashCode(conn))
      tryWithResource(conn.createStatement()) { stmt =>
        settings.dialect.createTableStatements.foreach { stmtStr =>
          stmt.execute(stmtStr)
        }
        Done
      }
    }
  }

  def clearOffset(projectionId: ProjectionId): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      logger.debug(
        "clearing offset for [{}], using connection id [{}], using connection id [{}]",
        projectionId,
        System.identityHashCode(conn))
      tryWithResource(conn.prepareStatement(settings.dialect.clearOffsetStatement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)
        val i = stmt.executeUpdate()
        logger.debug(s"clearing offset for [{}] - executed statement returned [{}]", projectionId, i)
        Done
      }
    }
  }

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] =
    withConnection(jdbcSessionFactory) { conn =>

      if (verboseLogging)
        logger.debug("reading offset for [{}], using connection id [{}]", projectionId, System.identityHashCode(conn))

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

          val result =
            if (buffer.isEmpty) None
            else if (buffer.forall(_.mergeable)) {
              Some(
                fromStorageRepresentation[MergeableOffset[_], Offset](MultipleOffsets(buffer.toList))
                  .asInstanceOf[Offset])
            } else {
              buffer.find(_.id == projectionId).map(fromStorageRepresentation[Offset, Offset])
            }

          if (verboseLogging) logger.debug2("found offset [{}] for [{}]", result, projectionId)

          result
        }

      }
    }

  /**
   * Like saveOffset, but async. Useful for resetting an offset
   */
  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      saveOffsetBlocking(conn, projectionId, offset)
    }
  }

  /**
   * This method is explicitly made non-async because it's used together
   * with the users' handler code and run inside the same Future
   */
  def saveOffsetBlocking[Offset](conn: Connection, projectionId: ProjectionId, offset: Offset): Done = {

    if (verboseLogging)
      logger.debug("saving offset [{}], using connection id [{}]", offset, System.identityHashCode(conn))

    val now = new Timestamp(Instant.now(clock).toEpochMilli)

    val storageReps = toStorageRepresentation(projectionId, offset)

    // Statement.EXECUTE_FAILED  (-3) means statement failed
    // -2 means successful, but there is no information about it (that's driver dependent).
    // 0 means nothing inserted or updated,
    // any positive number indicates the num of rows affected.
    // What a mess!!
    def failedStatement(i: Int) = i == 0 || i == Statement.EXECUTE_FAILED

    def insertOrUpdate(singleOffset: SingleOffset): Unit = {
      val tryUpdateResult =
        tryWithResource(conn.prepareStatement(settings.dialect.updateStatement())) { stmt =>
          // SET
          stmt.setString(UpdateIndices.OFFSET, singleOffset.offsetStr)
          stmt.setString(UpdateIndices.MANIFEST, singleOffset.manifest)
          stmt.setBoolean(UpdateIndices.MERGEABLE, singleOffset.mergeable)
          stmt.setTimestamp(UpdateIndices.LAST_UPDATED, now)
          // WHERE
          stmt.setString(UpdateIndices.PROJECTION_NAME, singleOffset.id.name)
          stmt.setString(UpdateIndices.PROJECTION_KEY, singleOffset.id.key)

          stmt.executeUpdate()
        }

      if (verboseLogging) {
        logger.debug2("tried to update offset [{}], statement result [{}]", offset, tryUpdateResult)
      }

      if (failedStatement(tryUpdateResult)) {
        tryWithResource(conn.prepareStatement(settings.dialect.insertStatement())) { stmt =>
          // VALUES
          stmt.setString(InsertIndices.PROJECTION_NAME, singleOffset.id.name)
          stmt.setString(InsertIndices.PROJECTION_KEY, singleOffset.id.key)
          stmt.setString(InsertIndices.OFFSET, singleOffset.offsetStr)
          stmt.setString(InsertIndices.MANIFEST, singleOffset.manifest)
          stmt.setBoolean(InsertIndices.MERGEABLE, singleOffset.mergeable)
          stmt.setTimestamp(InsertIndices.LAST_UPDATED, now)

          val triedInsertResult = stmt.executeUpdate()

          if (verboseLogging)
            logger.debug2("tried to insert offset [{}], batch result [{}]", offset, triedInsertResult)

          // did we get any failure on inserts?!
          if (failedStatement(triedInsertResult)) {
            throw new RuntimeException(s"Failed to insert offset [$singleOffset]")
          }
        }
      }

    }

    storageReps match {
      case single: SingleOffset  => insertOrUpdate(single)
      case MultipleOffsets(many) => many.foreach(insertOrUpdate)
    }

    Done
  }

}
