/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.time.Clock
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
import slick.jdbc.JdbcProfile

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SlickOffsetStore[P <: JdbcProfile](
    val db: P#Backend#Database,
    val profile: P,
    clock: Clock) {
  import OffsetSerialization.MultipleOffsets
  import OffsetSerialization.SingleOffset
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation
  import profile.api._

  def this(db: P#Backend#Database, profile: P) =
    this(db, profile, Clock.systemUTC())

  def readOffset[Offset](projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action = offsetTable.filter(_.projectionName === projectionId.name).result.map { maybeRow =>
      maybeRow.map(row =>
        SingleOffset(ProjectionId(projectionId.name, row.projectionKey), row.manifest, row.offsetStr, row.mergeable))
    }

    val results = db.run(action)

    results.map {
      case Nil => None
      case reps if reps.forall(_.mergeable) =>
        Some(fromStorageRepresentation[MergeableOffset[_], Offset](MultipleOffsets(reps)).asInstanceOf[Offset])
      case reps =>
        reps.find(_.id == projectionId) match {
          case Some(rep) => Some(fromStorageRepresentation[Offset, Offset](rep))
          case _         => None
        }
    }
  }

  private def newRow[Offset](rep: SingleOffset, now: Instant): DBIO[_] =
    offsetTable.insertOrUpdate(OffsetRow(rep.id.name, rep.id.key, rep.offsetStr, rep.manifest, rep.mergeable, now))

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): slick.dbio.DBIO[_] = {
    val now: Instant = Instant.now(clock)
    toStorageRepresentation(projectionId, offset) match {
      case offset: SingleOffset  => newRow(offset, now)
      case MultipleOffsets(reps) => DBIO.sequence(reps.map(rep => newRow(rep, now)))
    }
  }

  class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, "AKKA_PROJECTION_OFFSET_STORE") {

    def projectionName = column[String]("PROJECTION_NAME", O.Length(255, varying = false))
    def projectionKey = column[String]("PROJECTION_KEY", O.Length(255, varying = false))
    def offset = column[String]("OFFSET", O.Length(255, varying = false))
    def manifest = column[String]("MANIFEST", O.Length(4))
    def mergeable = column[Boolean]("MERGEABLE")
    def lastUpdated = column[Instant]("LAST_UPDATED")
    def pk = primaryKey("PK_PROJECTION_ID", (projectionName, projectionKey))

    def * = (projectionName, projectionKey, offset, manifest, mergeable, lastUpdated).mapTo[OffsetRow]
  }

  case class OffsetRow(
      projectionName: String,
      projectionKey: String,
      offsetStr: String,
      manifest: String,
      mergeable: Boolean,
      lastUpdated: Instant)

  val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Done] =
    db.run(offsetTable.schema.createIfNotExists).map(_ => Done)(ExecutionContexts.parasitic)
}
