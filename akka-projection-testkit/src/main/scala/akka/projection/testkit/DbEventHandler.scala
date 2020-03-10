/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import akka.projection.scaladsl.{AsyncEventHandler, EventHandler}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

trait DbEventHandler[E] extends EventHandler[E, DBIO[Done]] { self =>

  override def onFailure(event: E, throwable: Throwable): DBIO[Done] = throw throwable

  override final def onEvent(event: E): DBIO[Done] =
    Try(handleEvent(event)).recover {
      case NonFatal(ex) => onFailure(event, ex)
    }.get


  /**
   * Lift this DbProjectionHandler return type from DBIO[Done] to Future[Done].
   *
   * This is useful when not using a 'transactional' projection. For instance, when using in
   * a Kafka Projection with committable offsets, the Result type must be Future[Done]
   *
   */
  def asAsyncHandler(implicit execCtx: ExecutionContext) = new AsyncEventHandler[E] {

    val logger = LoggerFactory.getLogger(this.getClass)

    override implicit def exc: ExecutionContext = execCtx

    override def handleEvent(event: E): Future[Done] = {
      logger.info(s"handling event $event")
      Database.run(self.handleEvent(event))
    }
  }
}
