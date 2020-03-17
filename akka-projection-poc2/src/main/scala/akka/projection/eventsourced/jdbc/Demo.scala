/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.jdbc

import java.sql.Connection

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors

object Demo {

  object ShoppingCart {
    trait Event
  }

  object ShoppingCartProjection {
    def start(system: ActorSystem[_]): Unit = {

      val eventProcessorId = "ShoppingCartProcessor"
      val tag = "CartSlice-1"

      implicit val ec = system.executionContext

      val projectionHandler = new JdbcSingleEventHandlerWithTxOffset[ShoppingCart.Event](eventProcessorId, tag) {
        override def getConnection(): Future[Connection] = ???

        override def onEvent(event: ShoppingCart.Event): Future[Done] = {
          for {
            c <- getConnection()
            _ <- saveEventProjection(c, event)
            _ <- saveOffset(c) // in same tx
          } yield Done
        }

        private def saveEventProjection(c: Connection, event: ShoppingCart.Event): Future[Done] = {
          // save something
          Future.successful(Done)
        }

      }

      val projection = JdbcEventSourcedProjection(system, eventProcessorId, tag, projectionHandler)

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
