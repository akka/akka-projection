/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.util.UUID

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.annotation.InternalApi
import akka.persistence.query
import akka.projection.ProjectionId
import slick.jdbc.JdbcProfile

/**
 * INTERNAL API
 */
@InternalApi private[akka] class SlickOffsetStore[P <: JdbcProfile](db: P#Backend#Database, profile: P) {

  import profile.api._

  // offset manifest
  private val StringM = "STR"
  private val LongM = "LNG"
  private val IntM = "INT"
  private val SequenceM = "SEQ"
  private val TimeBasedUUIDM = "TBU"

  def readOffset[Offset](projectionId: ProjectionId)(implicit ec: ExecutionContext): Future[Option[Offset]] = {
    val action =
      offsetTable.filter(_.projectionId === projectionId.id).result.headOption.map(row => fromRowToOffset[Offset](row))

    db.run(action)
  }

  def saveOffset[Offset](projectionId: ProjectionId, offset: Offset)(
      implicit ec: ExecutionContext): slick.dbio.DBIO[Done] = {
    val (offsetStr, manifest) = stringOffsetAndManifest(offset)
    offsetTable.insertOrUpdate(OffsetRow(projectionId.id, offsetStr, manifest)).map(_ => Done)
  }

  /**
   * Deserialize an offset from a result row.
   * The offset is converted from its string representation to its real time.
   */
  private def fromRowToOffset[Offset](row: Option[OffsetRow]): Option[Offset] = {
    row match {
      case Some(OffsetRow(_, offset, StringM))   => Some(offset.asInstanceOf[Offset])
      case Some(OffsetRow(_, offset, LongM))     => Some(offset.toLong.asInstanceOf[Offset])
      case Some(OffsetRow(_, offset, IntM))      => Some(offset.toInt.asInstanceOf[Offset])
      case Some(OffsetRow(_, offset, SequenceM)) => Some(query.Sequence(offset.toLong).asInstanceOf[Offset])
      case Some(OffsetRow(_, offset, TimeBasedUUIDM)) =>
        Some(query.TimeBasedUUID(UUID.fromString(offset)).asInstanceOf[Offset])
      case Some(OffsetRow(id, _, manifest)) =>
        throw new IllegalStateException(s"Unknown offset manifest [$manifest] for projection id [$id]")
      case None => None
    }
  }

  /**
   * Convert the offset to a tuple (String, String) where the first element is
   * the String representation of the offset and the second its manifest
   */
  private def stringOffsetAndManifest[Offset](offset: Offset): (String, String) = {
    offset match {
      case s: String                => s -> StringM
      case l: Long                  => l.toString -> LongM
      case i: Int                   => i.toString -> IntM
      case seq: query.Sequence      => seq.value.toString -> SequenceM
      case tbu: query.TimeBasedUUID => tbu.value.toString -> TimeBasedUUIDM
      case _                        => throw new IllegalArgumentException(s"Unsupported offset type, found [${offset.getClass}]")
    }
  }

  private class OffsetStoreTable(tag: Tag) extends Table[OffsetRow](tag, "AKKA_PROJECTION_OFFSET_STORE") {

    def projectionId = column[String]("PROJECTION_ID", O.Length(255, varying = false), O.PrimaryKey)
    def offset = column[String]("OFFSET", O.Length(255, varying = false))
    def manifest = column[String]("MANIFEST", O.Length(4))

    def * = (projectionId, offset, manifest).mapTo[OffsetRow]
  }

  case class OffsetRow(projectionId: String, offsetStr: String, manifest: String)
  private val offsetTable = TableQuery[OffsetStoreTable]

  def createIfNotExists: Future[Unit] =
    db.run(offsetTable.schema.createIfNotExists)
}
