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
import akka.projection.internal.OffsetStore
import slick.jdbc.JdbcProfile

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SlickOffsetStore[Offset, P <: JdbcProfile](
    val db: P#Backend#Database,
    val profile: P,
    clock: Clock)
    extends OffsetStore[Offset, slick.dbio.DBIO] {
  import OffsetSerialization.fromStorageRepresentation
  import OffsetSerialization.toStorageRepresentation
  import profile.api._

  def this(db: P#Backend#Database, profile: P) =
    this(db, profile, Clock.systemUTC())

  def readOffset(projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action =
      offsetTable.filter(_.projectionId === projectionId.id).result.headOption.map { maybeRow =>
        maybeRow.map(row => fromStorageRepresentation[Offset](row.offsetStr, row.manifest))
      }

    db.run(action)
  }

  def saveOffset(projectionId: ProjectionId, offset: Offset)(implicit ec: ExecutionContext): slick.dbio.DBIO[Done] = {
    val (offsetStr, manifest) = toStorageRepresentation(offset)
    val now = Instant.now(clock)
    offsetTable.insertOrUpdate(OffsetRow(projectionId.id, offsetStr, manifest, now)).map(_ => Done)
  }

  def saveOffsetAsync(projectionId: ProjectionId, offset: Offset)(implicit ec: ExecutionContext): Future[Done] =
    db.run(saveOffset(projectionId, offset))
      .mapTo[Done]

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
