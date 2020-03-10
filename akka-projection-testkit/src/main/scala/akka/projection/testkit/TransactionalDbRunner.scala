/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import akka.projection.scaladsl.{OffsetStore, ProjectionRunner}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}


class TransactionalDbRunner[Offset](name: String) extends ProjectionRunner[Offset, DBIO[Done]] {

  val logger = LoggerFactory.getLogger(this.getClass)

  // FIXME: not safe, we need to harden it as test cases get more evolved
  private var _lastOffset: Option[Offset] = None

  override def offsetStore: OffsetStore[Offset, DBIO[Done]] =
    new OffsetStore[Offset, DBIO[Done]] {

      override def readOffset(): Future[Option[Offset]] = {
        logger.info(s"reading offset for projection '$name' '${_lastOffset}'")
        // a real implementation would read it from a DB
        Future.successful(_lastOffset)
      }

      override def saveOffset(offset: Offset): DBIO[Done] = {
        logger.info(s"saving offset for projection '$name' '${_lastOffset}'")
        _lastOffset = Some(offset)
        DBIO(Done)
      }
    }

  def lastOffset: Option[Offset] = _lastOffset

  override def run(offset: Offset)(handler: () => DBIO[Done])(implicit ec: ExecutionContext): Future[Done] = {
    logger.info(s"saving offset '$offset' for projection '$name'")
    // a real implementation would run the DBIO on a real DB
    val dbio = handler().flatMap(_ => offsetStore.saveOffset(offset))
    Database.run(dbio).map(_ => Done)
  }
}