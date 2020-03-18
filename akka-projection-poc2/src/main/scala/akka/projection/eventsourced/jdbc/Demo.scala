/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.eventsourced.jdbc

import java.sql.Connection

import scala.concurrent.Future
import scala.util.control.NonFatal

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

      val projectionHandler: JdbcSingleEventHandlerWithTxOffset[ShoppingCart.Event] =
        new JdbcSingleEventHandlerWithTxOffset[ShoppingCart.Event](eventProcessorId, tag) {
          override def getConnection(): Connection = ???

          override def onEvent(event: ShoppingCart.Event): Future[Done] = {
            val c = getConnection()
            try {
              saveEventProjection(c, event)
              saveOffset(c) // in same tx
              c.commit()
              Future.successful(Done)
            } catch {
              case NonFatal(e) =>
                c.rollback()
                throw e
            } finally {
              c.close()
            }
          }

          private def saveEventProjection(c: Connection, event: ShoppingCart.Event): Unit = {
            // save something
            ()
          }

        }

      val projection = JdbcEventSourcedProjection.onceAndOnlyOnce(system, eventProcessorId, tag, projectionHandler)

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
