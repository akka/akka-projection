/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import akka.Done
import akka.projection.scaladsl.OffsetManagement

import scala.concurrent.{ExecutionContext, Future}


class TransactionalOffsetManagement extends OffsetManagement[Long, DBIO[Done]] {

  private var lastOffset: Option[Long] = None

  def save(offset: Long): DBIO[Done] = {
    lastOffset = Some(offset)
    DBIO(Done)
  }

  override def readOffset(name: String): Future[Option[Long]] = {
    println(s"reading offset for projection '$name', '$lastOffset'")
    // a real implementation would read it from a DB
    Database.run(DBIO(lastOffset))
  }

  override def run(name: String, offset: Long)
                  (block: => DBIO[Done])(implicit ec: ExecutionContext): Future[Done] = {

    println(s"saving offset '$offset' for projection '$name'")

    // a real implementation would run the DBIO on a real DB
    val dbio = block.flatMap(_ => save(offset))
    Database.run(dbio).map(_ => Done)
  }
}