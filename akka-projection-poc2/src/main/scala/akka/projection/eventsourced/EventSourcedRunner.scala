/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.persistence.query.Offset
import akka.projection.scaladsl.ProjectionRunner

abstract class EventSourcedRunner extends ProjectionRunner[Offset, Future[Done]] {

  override def run(offset: Offset)(handler: () => Future[Done])(implicit ec: ExecutionContext): Future[Done] = {
    // FIXME this is inconvenient to not do in the stream, e.g. with groupedWithin.
    // Maybe have a few built in offset save strategies instead?
    handler().flatMap(_ => offsetStore.saveOffset(offset))
  }
}
