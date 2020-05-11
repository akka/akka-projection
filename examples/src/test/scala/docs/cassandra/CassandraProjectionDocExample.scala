/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.cassandra

import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import docs.eventsourced.ShoppingCart

//#projection-imports
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.ProjectionId

//#projection-imports

//#handler-imports
import akka.projection.scaladsl.Handler
import akka.Done
import org.slf4j.LoggerFactory
import scala.concurrent.Future

//#handler-imports

class CassandraProjectionDocExample {

  private val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  //#handler
  class ShoppingCartHandler extends Handler[EventEnvelope[ShoppingCart.Event]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ShoppingCart.Event]): Future[Done] = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info("Shopping cart {} was checked out at {}", cartId, time)
          Future.successful(Done)

        case otherEvent =>
          logger.debug("Shopping cart {} changed by {}", otherEvent.cartId, otherEvent)
          Future.successful(Done)
      }
    }
  }
  //#handler

  //#sourceProvider
  val sourceProvider =
    EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = CassandraReadJournal.Identifier, tag = "carts-1")
  //#sourceProvider

  object IllustrateAtLeastOnce {
    //#atLeastOnce
    val projection =
      CassandraProjection.atLeastOnce(
        projectionId = ProjectionId("ShoppingCarts", "carts-1"),
        sourceProvider,
        saveOffsetAfterEnvelopes = 100,
        saveOffsetAfterDuration = 500.millis,
        handler = new ShoppingCartHandler)
    //#atLeastOnce
  }

  object IllustrateAtMostOnce {
    //#atMostOnce
    val projection =
      CassandraProjection.atMostOnce(
        projectionId = ProjectionId("ShoppingCarts", "carts-1"),
        sourceProvider,
        handler = new ShoppingCartHandler)
    //#atMostOnce
  }
}
