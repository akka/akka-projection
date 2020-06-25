/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.jdbc

import java.time.Instant
import scala.concurrent.duration._

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.japi.function
import akka.persistence.jdbc.query.scaladsl.JdbcReadJournal

//#handler-imports
import akka.projection.jdbc.scaladsl.JdbcHandler

//#handler-imports

//#projection-imports
import akka.projection.ProjectionId
import akka.projection.jdbc.scaladsl.JdbcProjection

//#projection-imports

import akka.projection.eventsourced.EventEnvelope
import akka.projection.eventsourced.scaladsl.EventSourcedProvider

//#jdbc-session-imports
import java.sql.Connection
import java.sql.DriverManager
import akka.projection.jdbc.JdbcSession

//#jdbc-session-imports

import docs.eventsourced.ShoppingCart
import org.slf4j.LoggerFactory

object JdbcProjectionDocExample {

  //#repository
  case class Order(id: String, time: Instant)
  trait OrderRepository {
    def save(connection: Connection, order: Order): Unit
  }
  //#repository

  class OrderRepositoryImpl extends OrderRepository {
    override def save(connection: Connection, order: Order): Unit = ???
  }
  val orderRepository = new OrderRepositoryImpl

  // #jdbc-session
  class PlainJdbcSession extends JdbcSession {

    lazy val conn = {
      Class.forName("org.h2.Driver")
      val c = DriverManager.getConnection("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1")
      c.setAutoCommit(false)
      c
    }
    override def withConnection[Result](func: function.Function[Connection, Result]): Result =
      func(conn)
    override def commit(): Unit = conn.commit()
    override def rollback(): Unit = conn.rollback()
    override def close(): Unit = conn.close()
  }
  // #jdbc-session

  //#handler
  class ShoppingCartHandler(repository: OrderRepository)
      extends JdbcHandler[EventEnvelope[ShoppingCart.Event], PlainJdbcSession] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(session: PlainJdbcSession, envelope: EventEnvelope[ShoppingCart.Event]): Unit = {
      envelope.event match {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info(s"Shopping cart $cartId was checked out at $time")
          session.withConnection { conn =>
            repository.save(conn, Order(cartId, time))
          }

        case otherEvent =>
          logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
      }
    }
  }
  //#handler

  //#grouped-handler
  import scala.collection.immutable

  class GroupedShoppingCartHandler(repository: OrderRepository)
      extends JdbcHandler[immutable.Seq[EventEnvelope[ShoppingCart.Event]], PlainJdbcSession] {
    private val logger = LoggerFactory.getLogger(getClass)

    override def process(
        session: PlainJdbcSession,
        envelopes: immutable.Seq[EventEnvelope[ShoppingCart.Event]]): Unit = {

      // save all events in DB
      envelopes.map(_.event).foreach {
        case ShoppingCart.CheckedOut(cartId, time) =>
          logger.info(s"Shopping cart $cartId was checked out at $time")
          session.withConnection { conn =>
            repository.save(conn, Order(cartId, time))
          }

        case otherEvent =>
          logger.debug(s"Shopping cart ${otherEvent.cartId} changed by $otherEvent")
      }
    }
  }
  //#grouped-handler

  //#actor-system
  implicit val system = ActorSystem[Nothing](Behaviors.empty, "Example")
  //#actor-system

  //#sourceProvider
  val sourceProvider =
    EventSourcedProvider
      .eventsByTag[ShoppingCart.Event](system, readJournalPluginId = JdbcReadJournal.Identifier, tag = "carts-1")
  //#sourceProvider

  object IllustrateExactlyOnce {
    //#exactlyOnce
    implicit val ec = system.executionContext

    val projection =
      JdbcProjection
        .exactlyOnce(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          () => new PlainJdbcSession, // JdbcSession Factory
          handler = () => new ShoppingCartHandler(orderRepository))
    //#exactlyOnce
  }

  object IllustrateAtLeastOnce {
    //#atLeastOnce
    implicit val ec = system.executionContext

    val projection =
      JdbcProjection
        .atLeastOnce(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          () => new PlainJdbcSession, // JdbcSession Factory
          handler = () => new ShoppingCartHandler(orderRepository))
        .withSaveOffset(afterEnvelopes = 100, afterDuration = 500.millis)
    //#atLeastOnce
  }

  object IllustrateGrouped {
    //#grouped
    implicit val ec = system.executionContext

    val projection =
      JdbcProjection
        .groupedWithin(
          projectionId = ProjectionId("ShoppingCarts", "carts-1"),
          sourceProvider,
          () => new PlainJdbcSession, // JdbcSession Factory
          handler = () => new GroupedShoppingCartHandler(orderRepository))
        .withGroup(groupAfterEnvelopes = 20, groupAfterDuration = 500.millis)
    //#grouped
  }

}
