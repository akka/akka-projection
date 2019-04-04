/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.transactional

import akka.Done
import akka.projection.scaladsl.{OffsetStore, ProjectionRunner}

import scala.concurrent.{ExecutionContext, Future}


class TransactionalDbRunner(name: String) extends ProjectionRunner[Long, DBIO[Done]] {

  override def offsetStore: OffsetStore[Long, DBIO[Done]] =
    new OffsetStore[Long, DBIO[Done]] {
      private var lastOffset: Option[Long] = None

      override def readOffset(): Future[Option[Long]] = {
        println(s"reading offset for projection '$name' '$lastOffset'")
        // a real implementation would read it from a DB
        Database.run(DBIO(lastOffset))
      }

      override def saveOffset(offset: Long): DBIO[Done] = {
        lastOffset = Some(offset)
        DBIO(Done)
      }
    }

  override def run(offset: Long)(handler: => DBIO[Done])(implicit ec: ExecutionContext): Future[Done] = {
    println(s"saving offset '$offset' for projection '$name'")
    // a real implementation would run the DBIO on a real DB
    val dbio = handler.flatMap(_ => offsetStore.saveOffset(offset))
    Database.run(dbio).map(_ => Done)
  }
}