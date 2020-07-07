/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.time.Clock
import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.internal.OffsetSerialization
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

  private val offsetSerialization = new OffsetSerialization(system)
  import offsetSerialization.fromStorageRepresentation
  import offsetSerialization.toStorageRepresentation

  def readOffset[Offset](projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action = offsetTable.filter(_.projectionName === projectionId.name).result.map { maybeRow =>
      maybeRow.map(row =>
        SingleOffset(ProjectionId(projectionId.name, row.projectionKey), row.manifest, row.offsetStr, row.mergeable))
    }

    val results = db.run(action)

    results.map {
      case Nil => None
      case reps if reps.forall(_.mergeable) =>
        Some(
          fromStorageRepresentation[MergeableOffset[_, _], Offset](MultipleOffsets(reps.toList)).asInstanceOf[Offset])
      case reps =>
        reps.find(_.id == projectionId).map(fromStorageRepresentation[Offset, Offset])
    }
  }

  private def newRow[Offset](rep: SingleOffset, now: Instant): DBIO[_] =
    offsetTable.insertOrUpdate(OffsetRow(rep.id.name, rep.id.key, rep.offsetStr, rep.manifest, rep.mergeable, now))

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset): slick.dbio.DBIO[_] = {
    val now: Instant = Instant.now(clock)
    toStorageRepresentation(projectionId, offset) match {
      case offset: SingleOffset => newRow(offset, now)
      case MultipleOffsets(reps) =>
        val actions = reps.map(rep => newRow(rep, now))
        DBIO.sequence(actions)
    }
  }

  def clearOffset(projectionId: ProjectionId): slick.dbio.DBIO[_] = {
    offsetTable.filter(row => row.projectionName === projectionId.name && row.projectionKey === projectionId.key).delete
  }

  class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, slickSettings.schema, slickSettings.table) {

    def projectionName = column[String]("PROJECTION_NAME", O.Length(255))
    def projectionKey = column[String]("PROJECTION_KEY", O.Length(255))
    def offset = column[String]("OFFSET", O.Length(255))
    def manifest = column[String]("MANIFEST", O.Length(4))
    def mergeable = column[Boolean]("MERGEABLE")
    def lastUpdated = column[Instant]("LAST_UPDATED")

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
      lastUpdated: Instant)

  val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Done] =
    db.run(offsetTable.schema.createIfNotExists).map(_ => Done)(ExecutionContexts.parasitic)
}
