/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.collection.immutable
import akka.Done
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

sealed trait ProjectionHandler[Event]

trait AbstractSingleEventHandler[Event] extends ProjectionHandler[Event] {
  def onEvent(event: Event): Future[Done]
}

final class SingleEventHandler[Event](eventHandler: Event => Future[Done]) extends AbstractSingleEventHandler[Event] {
  override def onEvent(event: Event): Future[Done] = eventHandler(event)
}

trait AbstractGroupedEventsHandler[Event] extends ProjectionHandler[Event] {
  def n: Int
  def d: FiniteDuration
  def onEvents(events: immutable.Seq[Event]): Future[Done]
}

final class GroupedEventsHandler[Event](
    override val n: Int,
    override val d: FiniteDuration,
    eventHandler: immutable.Seq[Event] => Future[Done])
    extends AbstractGroupedEventsHandler[Event] {
  override def onEvents(events: immutable.Seq[Event]): Future[Done] =
    eventHandler(events)
}

// FIXME another type could be FlowWithContext[Event, Offset], e.g. for Kafka publishing events to Kafka
