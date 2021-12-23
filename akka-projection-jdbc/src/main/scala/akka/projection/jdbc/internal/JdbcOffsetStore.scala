/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.sql.Connection
import java.time.Clock

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.jdbc.JdbcSession
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

  def this(system: ActorSystem[_], settings: JdbcSettings, jdbcSessionFactory: () => S) =
    this(system, settings, jdbcSessionFactory, Clock.systemUTC())

  val offsetStoreStatements = new JdbcOffsetStoreStatements(system, settings, clock)
  private[projection] implicit val executionContext: ExecutionContext = settings.executionContext

  def dropIfExists(): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      logger.debug("creating offset-store table, using connection id [{}]", System.identityHashCode(conn))
      tryWithResource(conn.createStatement()) { stmt =>
        stmt.execute(settings.dialect.dropTableStatement)
        stmt.execute(settings.dialect.dropManagementTableStatement)
        Done
      }
    }
  }

  def createIfNotExists(): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      logger.debug("creating offset-store table, using connection id [{}]", System.identityHashCode(conn))
      tryWithResource(conn.createStatement()) { stmt =>
        settings.dialect.createTableStatements.foreach { stmtStr =>
          stmt.execute(stmtStr)
        }
        settings.dialect.createManagementTableStatements.foreach { stmtStr =>
          stmt.execute(stmtStr)
        }
        Done
      }
    }
  }

  def clearOffset(projectionId: ProjectionId): Future[Done] =
    withConnection(jdbcSessionFactory) { conn =>
      offsetStoreStatements.clearOffset(conn, projectionId)
    }

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] =
    withConnection(jdbcSessionFactory) { conn =>
      offsetStoreStatements.readOffset[Offset](conn, projectionId)
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
  def saveOffsetBlocking[Offset](conn: Connection, projectionId: ProjectionId, offset: Offset): Done =
    offsetStoreStatements.saveOffsetBlocking(conn, projectionId, offset)

  def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] =
    withConnection(jdbcSessionFactory) { conn =>
      offsetStoreStatements.readManagementState(conn, projectionId)
    }

  def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] = {
    withConnection(jdbcSessionFactory) { conn =>
      offsetStoreStatements.savePaused(conn, projectionId, paused)
    }
  }

}
