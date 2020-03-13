/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.persistence.cassandra.session.scaladsl.CassandraSession
import akka.persistence.query.Offset
import akka.persistence.query.TimeBasedUUID
import akka.projection.scaladsl.OffsetStore
import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Row

class CassandraOffsetStore(session: CassandraSession, eventProcessorId: String, tag: String)
    extends OffsetStore[Offset] {

  private implicit val ec: ExecutionContext = session.ec

  override def readOffset(): Future[Option[Offset]] = {
    session
      .selectOne(
        "SELECT timeUuidOffset FROM akka_cqrs_sample.offsetStore WHERE eventProcessorId = ? AND tag = ?",
        eventProcessorId,
        tag)
      .map(extractOffset)
  }

  private def extractOffset(maybeRow: Option[Row]): Option[Offset] = {
    maybeRow match {
      case Some(row) =>
        val uuid = row.getUUID("timeUuidOffset")
        if (uuid == null) {
          None
        } else {
          Some(TimeBasedUUID(uuid))
        }
      case None => None
    }
  }

  override def saveOffset(offset: Offset): Future[Done] = {
    offset match {
      case t: TimeBasedUUID =>
        prepareWriteOffset().map(stmt => stmt.bind(eventProcessorId, tag, t.value)).flatMap { boundStmt =>
          session.executeWrite(boundStmt)
        }

      case _ =>
        throw new IllegalArgumentException(s"Unexpected offset type $offset")
    }
  }

  private def prepareWriteOffset(): Future[PreparedStatement] = {
    session.prepare("INSERT INTO akka_cqrs_sample.offsetStore (eventProcessorId, tag, timeUuidOffset) VALUES (?, ?, ?)")
  }

}
