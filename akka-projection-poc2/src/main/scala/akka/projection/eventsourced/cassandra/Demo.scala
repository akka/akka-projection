/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.cassandra

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.GroupedEventsHandler
import akka.projection.scaladsl.SingleEventHandler

object Demo {

  object ShoppingCart {
    trait Event
  }

  object ShoppingCartProjection {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: EventEnvelope[ShoppingCart.Event] => Future[Done] = { eventEnvelope =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = new SingleEventHandler[EventEnvelope[ShoppingCart.Event]](eventHandler)

      implicit val ec = system.executionContext
      val projection = CassandraEventSourcedProjection.atLeastOnce(
        system,
        eventProcessorId,
        tag,
        projectionHandler,
        afterNumberOfEvents = 100,
        orAfterDuration = 250.millis)

      projection.start()
    }

  }

  object ShoppingCartProjectionWithGroupedEvents {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: immutable.Seq[EventEnvelope[ShoppingCart.Event]] => Future[Done] = { eventEnvelopes =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = new GroupedEventsHandler[EventEnvelope[ShoppingCart.Event]](10, 100.millis, eventHandler)

      implicit val ec = system.executionContext
      val projection = CassandraEventSourcedProjection.atLeastOnce(
        system,
        eventProcessorId,
        tag,
        projectionHandler,
        afterNumberOfEvents = 100,
        orAfterDuration = 250.millis)

      projection.start()
    }

  }

  object Guardian {
    def apply(): Behavior[Nothing] = {
      Behaviors.setup[Nothing] { context =>
        ShoppingCartProjection.start(context.system)
        Behaviors.empty
      }
    }
  }

  def main(args: Array[String]): Unit = {
    ActorSystem[Nothing](Guardian(), "Demo")
  }

}
