/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.projection.scaladsl.AsyncProjectionHandler

class EventSourcedHandler[Event](eventHandler: Event => Future[Done])(implicit override val ec: ExecutionContext)
    extends AsyncProjectionHandler[Event] {

  override def handleEvent(event: Event): Future[Done] = eventHandler(event)

}
