/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import scala.concurrent.Future
import scala.collection.immutable
import scala.concurrent.duration._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.eventsourced.cassandra.CassandraEventSourcedProjection
import akka.projection.scaladsl.GroupedEventsHandler
import akka.projection.scaladsl.OffsetStore
import akka.projection.scaladsl.SingleEventHandler

object Demo {

  object ShoppingCart {
    trait Event
  }

  object ShoppingCartProjection {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: ShoppingCart.Event => Future[Done] = { event =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = SingleEventHandler(eventHandler)

      implicit val ec = system.executionContext
      val offsetStrategy = OffsetStore.AtLeastOnce(100, 250.millis)
      val projection = CassandraEventSourcedProjection(system, eventProcessorId, tag, projectionHandler, offsetStrategy)

      projection.start()
    }

  }

  object ShoppingCartProjectionWithGroupedEvents {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: immutable.Seq[ShoppingCart.Event] => Future[Done] = { events =>
        // do something
        Future.successful(Done)
      }
      val projectionHandler = GroupedEventsHandler(10, 100.millis, eventHandler)

      implicit val ec = system.executionContext
      val offsetStrategy = OffsetStore.AtLeastOnce(100, 250.millis)
      val projection = CassandraEventSourcedProjection(system, eventProcessorId, tag, projectionHandler, offsetStrategy)

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
