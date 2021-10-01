/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.internal

import java.time.Clock
import java.time.Instant

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.ManagementState
import akka.projection.internal.OffsetSerialization
import akka.projection.internal.OffsetSerialization.MultipleOffsets
import akka.projection.internal.OffsetSerialization.SingleOffset
import akka.projection.r2dbc.R2dbcProjectionSettings
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Statement
import org.slf4j.LoggerFactory

object R2dbcOffsetStore {
  type SeqNr = Long
  type Pid = String

  final case class Record(pid: Pid, seqNr: SeqNr, timestamp: Instant)

  object State {
    val empty: State = State(Map.empty, Vector.empty, Instant.EPOCH)
  }

  final case class State(byPid: Map[Pid, Record], latest: immutable.IndexedSeq[Record], oldestTimestamp: Instant) {
    def latestTimestamp: Instant =
      if (latest.isEmpty) Instant.EPOCH
      else latest.head.timestamp
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class R2dbcOffsetStore(
    projectionId: ProjectionId,
    system: ActorSystem[_],
    settings: R2dbcProjectionSettings,
    r2dbcExecutor: R2dbcExecutor,
    clock: Clock) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val verboseLogging = logger.isDebugEnabled() && settings.verboseLoggingEnabled

  def this(
      projectionId: ProjectionId,
      system: ActorSystem[_],
      settings: R2dbcProjectionSettings,
      r2dbcExecutor: R2dbcExecutor) =
    this(projectionId, system, settings, r2dbcExecutor, Clock.systemUTC())

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  private val table = settings.tableWithSchema

  private[projection] implicit val executionContext: ExecutionContext = system.executionContext

  private val selectOffsetSql: String =
    s"""SELECT * FROM $table WHERE projection_name = $$1"""

  private val upsertOffsetSql: String =
    s"""INSERT INTO $table (
     |  projection_name,
     |  projection_key,
     |  current_offset,
     |  manifest,
     |  mergeable,
     |  last_updated
     |) VALUES ($$1,$$2,$$3,$$4,$$5,$$6)
     |ON CONFLICT (projection_name, projection_key)
     |DO UPDATE SET
     | current_offset = excluded.current_offset,
     | manifest = excluded.manifest,
     | mergeable = excluded.mergeable,
     | last_updated = excluded.last_updated
     |""".stripMargin

  private val clearOffsetSql: String =
    s"""DELETE FROM $table WHERE projection_name = $$1 AND projection_key = $$2"""

  def readOffset[Offset](): Future[Option[Offset]] = {
    val singleOffsets = r2dbcExecutor.select("read offset")(
      conn => {
        if (verboseLogging)
          logger.debug("reading offset for [{}], using connection id [{}]", projectionId, System.identityHashCode(conn))
        conn
          .createStatement(selectOffsetSql)
          .bind(0, projectionId.name)
      },
      row => {
        val offsetStr = row.get("current_offset", classOf[String])
        val manifest = row.get("manifest", classOf[String])
        val mergeable = row.get("mergeable", classOf[java.lang.Boolean])
        val key = row.get("projection_key", classOf[String])

        val adaptedProjectionId = ProjectionId(projectionId.name, key)
        SingleOffset(adaptedProjectionId, manifest, offsetStr, mergeable)
      })

    singleOffsets.map { offsets =>
      val result =
        if (offsets.isEmpty) None
        else if (offsets.forall(_.mergeable)) {
          Some(
            fromStorageRepresentation[MergeableOffset[_], Offset](MultipleOffsets(offsets.toList))
              .asInstanceOf[Offset])
        } else {
          offsets.find(_.id == projectionId).map(fromStorageRepresentation[Offset, Offset])
        }

      if (verboseLogging) logger.debug2("found offset [{}] for [{}]", result, projectionId)

      result
    }
  }

  /**
   * Like saveOffset, but in own transaction. Useful for resetting an offset
   */
  def saveOffset[Offset](offset: Offset): Future[Done] = {
    r2dbcExecutor
      .withConnection("save offset") { conn =>
        saveOffsetInTx(conn, offset)
      }
      .map(_ => Done)(ExecutionContext.parasitic)
  }

  /**
   * This method is used together with the users' handler code and run in same transaction.
   */
  def saveOffsetInTx[Offset](conn: Connection, offset: Offset): Future[Done] = {

    if (verboseLogging)
      logger.debug("saving offset [{}], using connection id [{}]", offset, System.identityHashCode(conn))

    val now = Instant.now(clock).toEpochMilli

    // FIXME can we move serialization outside the transaction?
    val storageReps = toStorageRepresentation(projectionId, offset)

    def upsertStmt(singleOffset: SingleOffset): Statement = {
      conn
        .createStatement(upsertOffsetSql)
        .bind("$1", singleOffset.id.name)
        .bind("$2", singleOffset.id.key)
        .bind("$3", singleOffset.offsetStr)
        .bind("$4", singleOffset.manifest)
        .bind("$5", java.lang.Boolean.valueOf(singleOffset.mergeable))
        .bind("$6", now)
    }

    val statements = storageReps match {
      case single: SingleOffset  => Vector(upsertStmt(single))
      case MultipleOffsets(many) => many.map(upsertStmt).toVector
    }

    R2dbcExecutor.updateInTx(statements).map(_ => Done)(ExecutionContext.parasitic)
  }

  def dropIfExists(): Future[Done] = {
    // FIXME not implemented yet
    Future.successful(Done)
  }

  def createIfNotExists(): Future[Done] = {
    // FIXME not implemented yet
    Future.successful(Done)
  }

  def clearOffset(): Future[Done] = {
    r2dbcExecutor
      .updateOne("clear offset") { conn =>
        logger.debug(
          "clearing offset for [{}], using connection id [{}], using connection id [{}]",
          projectionId,
          System.identityHashCode(conn))
        conn
          .createStatement(clearOffsetSql)
          .bind("$1", projectionId.name)
          .bind("$2", projectionId.key)
      }
      .map { n =>
        logger.debug(s"clearing offset for [{}] - executed statement returned [{}]", projectionId, n)
        Done
      }
  }

  def readManagementState(): Future[Option[ManagementState]] = {
    Future.successful(None) // FIXME not implemented yet
  }

  def savePaused(paused: Boolean): Future[Done] = {
    Future.successful(Done) // FIXME not implemented yet
  }

}
