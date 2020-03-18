/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.jdbc

import java.sql.Connection

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.persistence.query.Offset
import akka.persistence.query.Sequence
import akka.projection.scaladsl.AbstractGroupedEventsHandler
import akka.projection.scaladsl.AbstractSingleEventHandler
import akka.projection.scaladsl.OffsetManagedByProjectionHandler

abstract class JdbcSingleEventHandlerWithTxOffset[Event](eventProcessorId: String, tag: String)(
    implicit ec: ExecutionContext)
    extends JdbcProjectionHandlerWithTxOffset[Offset](eventProcessorId, tag)(ec)
    with AbstractSingleEventHandler[Event] {

  override def onEvent(event: Event): Future[Done]

}

abstract class JdbcGroupedEventsHandlerWithTxOffset[Event](
    eventProcessorId: String,
    tag: String,
    override val n: Int,
    override val d: FiniteDuration)(implicit ec: ExecutionContext)
    extends JdbcProjectionHandlerWithTxOffset[Offset](eventProcessorId, tag)(ec)
    with AbstractGroupedEventsHandler[Event] {

  def onEvents(events: immutable.Seq[Event]): Future[Done]

}

abstract class JdbcProjectionHandlerWithTxOffset[Event](eventProcessorId: String, tag: String)(
    implicit val ec: ExecutionContext)
    extends OffsetManagedByProjectionHandler[Offset] {

  private var currentOffset: Offset = Offset.noOffset

  def getConnection(): Connection

  override def readOffset(): Future[Option[Offset]] = {
    val c = getConnection()
    val pstmt =
      c.prepareStatement("SELECT offset FROM akka_cqrs_sample.offsetStore WHERE eventProcessorId = ? AND tag = ?")
    pstmt.setString(1, eventProcessorId)
    pstmt.setString(2, tag)
    val rs = pstmt.executeQuery()
    try {
      if (rs.next()) {
        // FIXME other types of offsets
        Future.successful(Some(Offset.sequence(rs.getLong(1))))
      } else {
        Future.successful(None)
      }
    } finally {
      // FIXME better resource closing
      rs.close()
      pstmt.close()
      c.close()
    }
  }

  override final def setCurrentOffset(offset: Offset): Unit = {
    currentOffset = offset
  }

  def getCurrentOffset: Offset =
    currentOffset

  def saveOffset(c: Connection): Unit = {
    currentOffset match {
      case Sequence(value) =>
        val pstmt = c.prepareStatement(
          "INSERT INTO akka_cqrs_sample.offsetStore (eventProcessorId, tag, offset) VALUES (?, ?, ?)")
        try {
          pstmt.setString(1, eventProcessorId)
          pstmt.setString(2, tag)
          pstmt.setLong(3, value)
          pstmt.execute()
        } finally {
          // FIXME better resource closing
          pstmt.close()
        }
      case _ =>
        // FIXME other types of offsets
        throw new IllegalArgumentException("Only Sequence Offset supported.")
    }
  }

}
