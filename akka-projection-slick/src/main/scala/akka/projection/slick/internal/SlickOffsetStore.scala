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
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation
  import profile.api._

  def this(db: P#Backend#Database, profile: P) = this(db, profile, Clock.systemUTC())

  def readOffset[Offset](projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action =
      offsetTable.filter(_.projectionId === projectionId.id).result.headOption.map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.offsetStr, row.manifest))
      }

    db.run(action)
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset)(
      implicit ec: ExecutionContext): slick.dbio.DBIO[Done] = {
    val (offsetStr, manifest) = toStorageRepresentation(offset)
    val now = Instant.now(clock)
    offsetTable.insertOrUpdate(OffsetRow(projectionId.id, offsetStr, manifest, now)).map(_ => Done)
  }

  class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, "AKKA_PROJECTION_OFFSET_STORE") {

    def projectionId = column[String]("PROJECTION_ID", O.Length(255, varying = false), O.PrimaryKey)
    def offset = column[String]("OFFSET", O.Length(255, varying = false))
    def manifest = column[String]("MANIFEST", O.Length(4))
    def lastUpdated = column[Instant]("LAST_UPDATED")

    def * = (projectionId, offset, manifest, lastUpdated).mapTo[OffsetRow]
  }

  case class OffsetRow(projectionId: String, offsetStr: String, manifest: String, lastUpdated: Instant)

  val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Unit] =
    db.run(offsetTable.schema.createIfNotExists)
}
