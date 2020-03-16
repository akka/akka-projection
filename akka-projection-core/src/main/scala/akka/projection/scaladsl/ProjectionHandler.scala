/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.collection.immutable
import akka.Done
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

sealed trait ProjectionHandler[Event]

final case class SingleEventHandler[Event](eventHandler: Event => Future[Done]) extends ProjectionHandler[Event]

final case class GroupedEventsHandler[Event](
    n: Int,
    d: FiniteDuration,
    eventHandler: immutable.Seq[Event] => Future[Done])
    extends ProjectionHandler[Event]

// FIXME another type could be FlowWithContext[Event, Offset], e.g. for Kafka publishing events to Kafka
