/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.slick

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal
import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import docs.eventsourced.ShoppingCart

//#projection-imports
import akka.projection.ProjectionId
import akka.projection.slick.SlickProjection
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.H2Profile

//#projection-imports

//#handler-imports
import scala.concurrent.Future

import akka.Done
import akka.projection.slick.SlickHandler
import org.slf4j.LoggerFactory

//#handler-imports

class SlickProjectionDocExample {

  //#repository
  case class Order(id: String, time: Instant)

  class OrderRepository(val dbConfig: DatabaseConfig[H2Profile]) {

    import dbConfig.profile.api._

    private class OrdersTable(tag: Tag) extends Table[Order](tag, "ORDERS") {
      def id = column[String]("CART_ID", O.PrimaryKey)

      def time = column[Instant]("TIME")

      def * = (id, time).mapTo[Order]
    }

    private val ordersTable = TableQuery[OrdersTable]

    def save(order: Order)(implicit ec: ExecutionContext) = {
      ordersTable.insertOrUpdate(order).map(_ => Done)
    }

    def createTable(): Future[Unit] =
      dbConfig.db.run(ordersTable.schema.createIfNotExists)
  }
  //#repository

  //#handler
  class ShoppingCartHandler(repository: OrderRepository)(implicit ec: ExecutionContext)
      extends SlickHandler[EventEnvelope[ShoppingCart.Event]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ShoppingCart.Event]): DBIO[Done] = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info(s"Shopping cart $cartId was checked out at $time")
          repository.save(Order(cartId, time))

        case otherEvent =>
          logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
          DBIO.successful(Done)
      }
    }
  }
  //#handler

  //#grouped-handler
  import scala.collection.immutable

  class GroupedShoppingCartHandler(repository: OrderRepository)(implicit ec: ExecutionContext)
      extends SlickHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]]] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(envelopes: immutable.Seq[EventEnvelope[ShoppingCart.Event]]): DBIO[Done] = {
      val dbios = envelopes.map(_.event).map {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info(s"Shopping cart $cartId was checked out at $time")
          repository.save(Order(cartId, time))

        case otherEvent =>
          logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
          DBIO.successful(Done)
      }
      DBIO.sequence(dbios).map(_ => Done)
    }
  }
  //#grouped-handler

  implicit val system = ActorSystem[Nothing](Behaviors.empty, "Example")

  //#db-config
  val dbConfig: DatabaseConfig[H2Profile] = DatabaseConfig.forConfig("akka.projection.slick", system.settings.config)

  val repository = new OrderRepository(dbConfig)
  //#db-config

  //#sourceProvider
  val sourceProvider =
    EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = JdbcReadJournal.Identifier, tag = "carts-1")
  //#sourceProvider

  object IllustrateExactlyOnce {
    //#exactlyOnce
    implicit val ec = system.executionContext

    val projection =
      SlickProjection.exactlyOnce(
        projectionId = ProjectionId("ShoppingCarts", "carts-1"),
        sourceProvider,
        dbConfig,
        handler = () => new ShoppingCartHandler(repository))
    //#exactlyOnce
  }

  object IllustrateAtLeastOnce {
    //#atLeastOnce
    implicit val ec = system.executionContext

    val projection =
      SlickProjection
        .atLeastOnce(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          dbConfig,
          handler = () => new ShoppingCartHandler(repository))
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#atLeastOnce
  }

  object IllustrateGrouped {
    //#grouped
    implicit val ec = system.executionContext

    val projection =
      SlickProjection
        .groupedWithin(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          dbConfig,
          handler = () => new GroupedShoppingCartHandler(repository))
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#grouped
  }

}
