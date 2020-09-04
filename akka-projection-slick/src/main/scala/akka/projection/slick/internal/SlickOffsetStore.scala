/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.sql.Connection
import java.sql.Statement
import java.time.Clock

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import akka.projection.jdbc.internal.DefaultDialect
import akka.projection.jdbc.internal.Dialect
import akka.projection.jdbc.internal.DialectDefaults.InsertIndices
import akka.projection.jdbc.internal.DialectDefaults.UpdateIndices
import akka.projection.jdbc.internal.JdbcSessionUtil
import akka.projection.jdbc.internal.JdbcSessionUtil.tryWithResource
import akka.projection.jdbc.internal.MSSQLServerDialect
import akka.projection.jdbc.internal.MySQLDialect
import akka.projection.jdbc.internal.OracleDialect
import org.slf4j.LoggerFactory
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
  import OffsetSerialization.MultipleOffsets
  import OffsetSerialization.SingleOffset
  import profile.api._

  def this(system: ActorSystem[_], db: P#Backend#Database, profile: P, slickSettings: SlickSettings) =
    this(system, db, profile, slickSettings, Clock.systemUTC())

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val verboseLogging = logger.isDebugEnabled() && slickSettings.verboseLoggingEnabled

  val dialect: Dialect =
    profile match {
      case _: slick.jdbc.MySQLProfile     => MySQLDialect(slickSettings.schema, slickSettings.table)
      case _: slick.jdbc.PostgresProfile  => DefaultDialect(slickSettings.schema, slickSettings.table)
      case _: slick.jdbc.H2Profile        => DefaultDialect(slickSettings.schema, slickSettings.table)
      case _: slick.jdbc.SQLServerProfile => MSSQLServerDialect(slickSettings.schema, slickSettings.table)
      case _: slick.jdbc.OracleProfile    => OracleDialect(slickSettings.schema, slickSettings.table)
    }

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  def readOffset[Offset](projectionId: ProjectionId): Future[Option[Offset]] = {
    val action =
      SimpleDBIO[Option[Offset]] { jdbcContext =>

        val connection = jdbcContext.connection

        if (verboseLogging)
          logger.debug(
            "reading offset for [{}], using connection id [{}]",
            projectionId,
            System.identityHashCode(connection))

        // init Statement in try-with-resource
        tryWithResource(connection.prepareStatement(dialect.readOffsetQuery)) { stmt =>

          stmt.setString(1, projectionId.name)

          // init ResultSet in try-with-resource
          tryWithResource(stmt.executeQuery()) { resultSet =>

            val buffer = ListBuffer.empty[SingleOffset]

            while (resultSet.next()) {

              val offsetStr = resultSet.getString("LAST_SEEN_OFFSET")
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
    db.run(action)
  }

  private def newRow[Offset](offset: SingleOffset, millisSinceEpoch: Long): DBIO[_] = {

    // Statement.EXECUTE_FAILED  (-3) means statement failed
    // -2 means successful, but there is no information about it (that's driver dependent).
    // 0 means nothing inserted or updated,
    // any positive number indicates the num of rows affected.
    // What a mess!!
    def failedStatement(i: Int) = i == 0 || i == Statement.EXECUTE_FAILED

    def insertOrUpdate(conn: Connection): Unit = {

      if (verboseLogging)
        logger.debug("saving offset [{}], using connection id [{}]", offset, System.identityHashCode(conn))

      val tryUpdateResult =
        tryWithResource(conn.prepareStatement(dialect.updateStatement())) { stmt =>
          // SET
          stmt.setString(UpdateIndices.LAST_SEEN_OFFSET, offset.offsetStr)
          stmt.setString(UpdateIndices.MANIFEST, offset.manifest)
          stmt.setBoolean(UpdateIndices.MERGEABLE, offset.mergeable)
          stmt.setLong(UpdateIndices.LAST_UPDATED, millisSinceEpoch)
          // WHERE
          stmt.setString(UpdateIndices.PROJECTION_NAME, offset.id.name)
          stmt.setString(UpdateIndices.PROJECTION_KEY, offset.id.key)

          stmt.executeUpdate()
        }

      if (verboseLogging) {
        logger.debug2("tried to update offset [{}], statement result [{}]", offset, tryUpdateResult)
      }

      if (failedStatement(tryUpdateResult)) {
        tryWithResource(conn.prepareStatement(dialect.insertStatement())) { stmt =>
          // VALUES
          stmt.setString(InsertIndices.PROJECTION_NAME, offset.id.name)
          stmt.setString(InsertIndices.PROJECTION_KEY, offset.id.key)
          stmt.setString(InsertIndices.LAST_SEEN_OFFSET, offset.offsetStr)
          stmt.setString(InsertIndices.MANIFEST, offset.manifest)
          stmt.setBoolean(InsertIndices.MERGEABLE, offset.mergeable)
          stmt.setLong(InsertIndices.LAST_UPDATED, millisSinceEpoch)

          val triedInsertResult = stmt.executeUpdate()

          if (verboseLogging)
            logger.debug2("tried to insert offset [{}], batch result [{}]", offset, triedInsertResult)

          // did we get any failure on inserts?!
          if (failedStatement(triedInsertResult)) {
            throw new RuntimeException(s"Failed to insert offset [$offset]")
          }
        }
      }

    }

    SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      insertOrUpdate(connection)
    }

  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): slick.dbio.DBIO[_] = {
    val millisSinceEpoch = clock.instant().toEpochMilli
    toStorageRepresentation(projectionId, offset) match {
      case rep: SingleOffset => newRow(rep, millisSinceEpoch)
      case MultipleOffsets(reps) =>
        val actions = reps.map(rep => newRow(rep, millisSinceEpoch))
        DBIO.sequence(actions)
    }
  }

  def clearOffset(projectionId: ProjectionId): slick.dbio.DBIO[_] = {
    SimpleDBIO[Unit] { jdbcContext =>

      val connection = jdbcContext.connection

      logger.debug(
        "clearing offset for [{}], using connection id [{}], using connection id [{}]",
        projectionId,
        System.identityHashCode(connection))

      JdbcSessionUtil.tryWithResource(connection.prepareStatement(dialect.clearOffsetStatement)) { stmt =>
        stmt.setString(1, projectionId.name)
        stmt.setString(2, projectionId.key)
        val i = stmt.executeUpdate()
        logger.debug(s"clearing offset for [{}] - executed statement returned [{}]", projectionId, i)
        Done
      }

    }

  }

  class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, slickSettings.schema, slickSettings.table) {

    def projectionName = column[String]("PROJECTION_NAME", O.Length(255))
    def projectionKey = column[String]("PROJECTION_KEY", O.Length(255))
    def offset = column[String]("LAST_SEEN_OFFSET", O.Length(255))
    def manifest = column[String]("MANIFEST", O.Length(4))
    def mergeable = column[Boolean]("MERGEABLE")
    def lastUpdated = column[Long]("LAST_UPDATED")

    def pk = primaryKey("PK_PROJECTION_ID", (projectionName, projectionKey))
    def idx = index("PROJECTION_NAME_INDEX", projectionName)

    def * = (projectionName, projectionKey, offset, manifest, mergeable, lastUpdated).mapTo[OffsetRow]
  }

  case class OffsetRow(
      projectionName: String,
      projectionKey: String,
      offsetStr: String,
      manifest: String,
      mergeable: Boolean,
      lastUpdated: Long)

  val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Done] = {
    val prepareSchemaDBIO = SimpleDBIO[Unit] { jdbcContext =>
      val connection = jdbcContext.connection
      dialect.createTableStatements.foreach { sql =>
        JdbcSessionUtil.tryWithResource(connection.createStatement()) { stmt =>
          stmt.execute(sql)
        }
      }
    }
    db.run(prepareSchemaDBIO).map(_ => Done)(ExecutionContexts.parasitic)
  }
}
