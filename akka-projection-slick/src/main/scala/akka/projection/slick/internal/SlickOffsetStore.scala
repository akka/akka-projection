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
  import OffsetSerialization.RawOffset
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation
  import profile.api._

  def this(db: P#Backend#Database, profile: P) =
    this(db, profile, Clock.systemUTC())

  def readOffset[Offset](projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action =
      offsetTable.filter(_.projectionName === projectionId.name).result.map { maybeRow =>
        maybeRow.map(row => RawOffset(projectionId, row.manifest, row.offsetStr))
      }

    val results = db.run(action)

    results.map { offsetRows =>
      if (offsetRows.isEmpty) None
      else fromStorageRepresentation[Offset](offsetRows, projectionId)
    }
  }

  private def newRow(projectionId: ProjectionId, offsetStr: String, manifest: String, now: Instant): DBIO[_] =
    offsetTable.insertOrUpdate(OffsetRow(projectionId.name, projectionId.key, offsetStr, manifest, now))

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset)(
      implicit ec: ExecutionContext): slick.dbio.DBIO[Done] = {
    val now: Instant = Instant.now(clock)
    // TODO: maybe there's a more "slick" way to accumulate DBIOs like this?
    toStorageRepresentation(offset)
      .foldLeft(None.asInstanceOf[Option[DBIO[_]]]) {
        case (None, (offset, manifest))      => Some(newRow(projectionId, offset, manifest, now))
        case (Some(acc), (offset, manifest)) => Some(acc.andThen(newRow(projectionId, offset, manifest, now)))
      }
      .get
      .map(_ => Done)
  }

  class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, "AKKA_PROJECTION_OFFSET_STORE") {

    def projectionName = column[String]("PROJECTION_NAME", O.Length(255, varying = false))
    def projectionKey = column[String]("PROJECTION_KEY", O.Length(255, varying = false))
    def offset = column[String]("OFFSET", O.Length(255, varying = false))
    def manifest = column[String]("MANIFEST", O.Length(4))
    def lastUpdated = column[Instant]("LAST_UPDATED")
    def pk = primaryKey("PK_PROJECTION_ID", (projectionName, projectionKey))

    def * = (projectionName, projectionKey, offset, manifest, lastUpdated).mapTo[OffsetRow]
  }

  case class OffsetRow(
      projectionName: String,
      projectionKey: String,
      offsetStr: String,
      manifest: String,
      lastUpdated: Instant)

  val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Unit] =
    db.run(offsetTable.schema.createIfNotExists)
}
