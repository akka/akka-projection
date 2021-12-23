/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.sql.Connection
import java.time.Clock

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.H2Dialect
import akka.projection.jdbc.internal.JdbcOffsetStoreStatements
import akka.projection.jdbc.internal.JdbcSessionUtil
import akka.projection.jdbc.internal.JdbcSettingsBase
import akka.projection.jdbc.internal.MSSQLServerDialect
import akka.projection.jdbc.internal.MySQLDialect
import akka.projection.jdbc.internal.OracleDialect
import akka.projection.jdbc.internal.PostgresDialect
import slick.jdbc.JdbcProfile

/**
 * INTERNAL API
 */
@InternalApi private[projection] class SlickOffsetStore[P <: JdbcProfile](
    system: ActorSystem[_],
    val db: P#Backend#Database,
    val profile: P,
    slickSettings: SlickSettings,
    clock: Clock) {
  import profile.api._

  def this(system: ActorSystem[_], db: P#Backend#Database, profile: P, slickSettings: SlickSettings) =
    this(system, db, profile, slickSettings, Clock.systemUTC())

  val (jdbcDialect, useLowerCase): (Dialect, Boolean) = {

    val useLowerCase = slickSettings.useLowerCase

    profile match {
      case _: slick.jdbc.H2Profile =>
        (
          H2Dialect(slickSettings.schema, slickSettings.table, slickSettings.managementTable, useLowerCase),
          useLowerCase)
      case _: slick.jdbc.PostgresProfile =>
        (
          PostgresDialect(slickSettings.schema, slickSettings.table, slickSettings.managementTable, useLowerCase),
          useLowerCase)
      // mysql and Sql server are case insensitive, we favor lower case
      case _: slick.jdbc.SQLServerProfile =>
        (MSSQLServerDialect(slickSettings.schema, slickSettings.table, slickSettings.managementTable), true)
      case _: slick.jdbc.MySQLProfile =>
        (MySQLDialect(slickSettings.schema, slickSettings.table, slickSettings.managementTable), true)
      // oracle must always use quoted + uppercase
      case _: slick.jdbc.OracleProfile =>
        (OracleDialect(slickSettings.schema, slickSettings.table, slickSettings.managementTable), false)

    }
  }

  val jdbcSettings = new JdbcSettingsBase {
    override val schema: Option[String] = jdbcDialect.schema
    override val table: String = jdbcDialect.tableName
    override val managementTable: String = jdbcDialect.managementTableName
    override val verboseLoggingEnabled: Boolean = slickSettings.verboseLoggingEnabled
    override val dialect: Dialect = jdbcDialect
  }

  val offsetStoreStatements = new JdbcOffsetStoreStatements(system, jdbcSettings, clock)

  private def simpleDBIO[T](func: Connection => T): DBIO[T] =
    SimpleDBIO[T] { jdbcContext => func(jdbcContext.connection) }

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    val action =
      simpleDBIO { conn =>
        offsetStoreStatements.readOffset(conn, projectionId)
      }
    db.run(action)
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): slick.dbio.DBIO[_] =
    simpleDBIO { conn =>
      offsetStoreStatements.saveOffsetBlocking(conn, projectionId, offset)
    }

  def clearOffset(projectionId: ProjectionId): slick.dbio.DBIO[_] =
    simpleDBIO { conn =>
      offsetStoreStatements.clearOffset(conn, projectionId)
    }

  def createIfNotExists(): Future[Done] = {
    val prepareSchemaDBIO = SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      jdbcDialect.createTableStatements.foreach(sql =>
        JdbcSessionUtil.tryWithResource(connection.createStatement()) { stmt =>
          stmt.execute(sql)
        })
    }
    val prepareManagementSchemaDBIO = SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      jdbcDialect.createManagementTableStatements.foreach(sql =>
        JdbcSessionUtil.tryWithResource(connection.createStatement()) { stmt =>
          stmt.execute(sql)
        })
    }
    db.run(DBIO.seq(prepareSchemaDBIO, prepareManagementSchemaDBIO)).map(_ => Done)(ExecutionContexts.parasitic)
  }

  def dropIfExists(): Future[Done] = {
    val prepareSchemaDBIO = SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      JdbcSessionUtil.tryWithResource(connection.createStatement()) { stmt =>
        stmt.execute(jdbcDialect.dropTableStatement)
      }
    }
    val prepareManagementSchemaDBIO = SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      JdbcSessionUtil.tryWithResource(connection.createStatement()) { stmt =>
        stmt.execute(jdbcDialect.dropManagementTableStatement)
      }
    }
    db.run(DBIO.seq(prepareSchemaDBIO, prepareManagementSchemaDBIO)).map(_ => Done)(ExecutionContexts.parasitic)
  }

  def readManagementState(projectionId: ProjectionId): Future[Option[ManagementState]] = {
    val action = simpleDBIO { conn =>
      offsetStoreStatements.readManagementState(conn, projectionId)
    }
    db.run(action)
  }

  def savePaused(projectionId: ProjectionId, paused: Boolean): Future[Done] = {
    val action = simpleDBIO { conn =>
      offsetStoreStatements.savePaused(conn, projectionId, paused)
    }
    db.run(action).map(_ => Done)(ExecutionContexts.parasitic)
  }
}
