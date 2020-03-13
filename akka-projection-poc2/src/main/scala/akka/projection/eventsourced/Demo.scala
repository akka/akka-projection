/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.eventsourced.cassandra.CassandraEventSourcedProjection

object Demo {

  object ShoppingCart {
    trait Event
  }

  object ShoppingCartProjection {
    def start(system: ActorSystem[_]) = {
      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      val eventHandler: ShoppingCart.Event => Future[Done] = { event =>
        // do something
        Future.successful(Done)
      }

      implicit val ec = system.executionContext
      val projection = CassandraEventSourcedProjection(system, eventProcessorId, tag, eventHandler)

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
