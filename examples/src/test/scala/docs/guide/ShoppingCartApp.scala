/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

//#guideSetup

import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.EventEnvelope
import com.typesafe.config.ConfigFactory
import docs.guide.DailyCheckoutProjectionHandler.CartState

//#guideSetup
//#guideSourceProviderImports

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider

//#guideSourceProviderImports

//#guideProjectionImports

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

import akka.Done
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.projection.scaladsl.StatefulHandler
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import org.slf4j.LoggerFactory

//#guideProjectionImports

//#guideSetup
object ShoppingCartEvents {

  /**
   * This interface defines all the events that the ShoppingCart supports.
   */
  sealed trait Event extends CborSerializable {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event
  final case class ItemRemoved(cartId: String, itemId: String) extends Event
  final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int) extends Event
  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event
}

object ShoppingCartApp extends App {
  val config = ConfigFactory.parseResources("guide-shopping-cart-app.conf")

  ActorSystem(
    Behaviors.setup[String] { context =>
      val system = context.system

      // ...

      //#guideSetup
      //#guideSourceProviderSetup
      val europeanShoppingCartsTag = "carts-eu"
      val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]] =
        EventSourcedProvider
          .eventsByTag[ShoppingCartEvents.Event](
            system,
            readJournalPluginId = CassandraReadJournal.Identifier,
            tag = europeanShoppingCartsTag)
      //#guideSourceProviderSetup

      //#guideProjectionSetup
      // FIXME: use a custom dispatcher for repo?
      implicit val ec = system.classicSystem.dispatcher
      val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
      val repo = new DailyCheckoutProjectionRepositoryImpl(session)
      val projection = CassandraProjection.atLeastOnce(
        projectionId = ProjectionId("shopping-carts", europeanShoppingCartsTag),
        sourceProvider,
        handler = () => new DailyCheckoutProjectionHandler(europeanShoppingCartsTag, system, repo))

      context.spawn(ProjectionBehavior(projection), projection.projectionId.id)
      //#guideProjectionSetup

      //#guideSetup
      Behaviors.empty
    },
    "ShoppingCartApp",
    config)
}
//#guideSetup

//#guideProjectionRepo
trait DailyCheckoutProjectionRepository {
  import DailyCheckoutProjectionHandler._

  def checkoutCountsForDate(date: LocalDate): Future[Seq[Checkout]]
  def addCheckout(checkoutItem: Checkout): Future[Done]
  def cartState(): Future[Seq[CartState]]
  def updateCartState(cartState: CartState): Future[Done]
  def deleteCartState(cartId: String): Future[Done]
}

class DailyCheckoutProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends DailyCheckoutProjectionRepository {
  import DailyCheckoutProjectionHandler._

  val keyspace = "akka_projection"
  val dailyCheckoutsTable = "daily_checkouts"
  val cartStateTable = "cart_state"

  def checkoutCountsForDate(date: LocalDate): Future[Seq[Checkout]] = {
    session
      .selectAll(s"SELECT cart_id, item_id, quantity FROM $keyspace.$dailyCheckoutsTable WHERE date = ?", date)
      .map { rows =>
        rows.map(row => Checkout(date, row.getString("cart_id"), row.getString("item_id"), row.getInt("quantity")))
      }
  }

  def addCheckout(checkoutItem: Checkout): Future[Done] = {
    import checkoutItem._
    session.executeWrite(
      s"INSERT INTO $keyspace.$dailyCheckoutsTable (date, cart_id, item_id, quantity) VALUES (?, ?, ?, ?)",
      date,
      cartId,
      itemId,
      Int.box(quantity))
  }

  def cartState(): Future[Seq[CartState]] = {
    session
      .selectAll(s"SELECT cart_id, item_id, quantity FROM $keyspace.$cartStateTable")
      .map { rows =>
        rows.map(row => CartState(row.getString("cart_id"), row.getString("item_id"), row.getInt("quantity")))
      }
  }

  def updateCartState(cartState: CartState): Future[Done] = {
    import cartState._
    session.executeWrite(
      s"UPDATE $keyspace.$cartStateTable SET quantity = ? WHERE cart_id = ? AND item_id = ?",
      Integer.valueOf(quantity),
      cartId,
      itemId)
  }

  def deleteCartState(cartId: String): Future[Done] = {
    session.executeWrite(s"DELETE FROM $keyspace.$cartStateTable WHERE cart_id = ?", cartId)
  }
}
//#guideProjectionRepo

//#guideProjectionHandler
object DailyCheckoutProjectionHandler {
  final case class Checkout(date: LocalDate, cartId: String, itemId: String, quantity: Int)
  final case class CartState(cartId: String, itemId: String, quantity: Int)

  def toDate(date: Instant) = LocalDate.from(date.atZone(ZoneId.of("UTC")))
}

class DailyCheckoutProjectionHandler(tag: String, system: ActorSystem[_], repo: DailyCheckoutProjectionRepository)
    extends StatefulHandler[Map[String, Map[String, CartState]], EventEnvelope[ShoppingCartEvents.Event]]()(
      system.classicSystem.dispatcher) {

  import DailyCheckoutProjectionHandler._

  @volatile private var checkoutCounter = 0
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.classicSystem.dispatcher

  def initialState(): Future[Map[String, Map[String, CartState]]] =
    repo.cartState().map { rows =>
      val byCartId = rows.groupBy(_.cartId)
      val byCartAndItem = byCartId.view
        .mapValues(_.groupBy(_.itemId).map { case (itemId, states) => (itemId -> states.head) })
        .toMap
      byCartAndItem
    }

  override def process(
      state: Map[String, Map[String, CartState]],
      envelope: EventEnvelope[ShoppingCartEvents.Event]): Future[Map[String, Map[String, CartState]]] = {
    envelope.event match {
      case ShoppingCartEvents.CheckedOut(cartId, eventTime) =>
        val dateOnly = toDate(eventTime)
        val cartItems = state.getOrElse(cartId, Map.empty)

        val updates = Future.sequence(cartItems.map {
          case (itemId, cartState) =>
            repo.addCheckout(Checkout(dateOnly, cartId, itemId, cartState.quantity))
        })

        checkoutCounter += 1
        val updatesWithCountStatus = if (checkoutCounter % 10 == 0) {
          updates.flatMap { _ =>
            val countQuery = repo.checkoutCountsForDate(dateOnly)
            countQuery.onComplete {
              case Success(checkouts) =>
                log.info(
                  "DailyCheckoutProjectionHandler({}) current checkouts for the day [{}] is: {}",
                  tag,
                  dateOnly,
                  dailyCheckouts(checkouts))
              case Failure(ex) =>
                log.error(s"ShoppingCartProjectionHandler($tag) an error occurred when connecting to the repo", ex)
            }
            countQuery
          }
        } else updates

        val newState = state - cartId
        Future
          .sequence(Seq(updatesWithCountStatus, repo.deleteCartState(cartId)))
          .map(_ => newState)

      case ShoppingCartEvents.ItemAdded(cartId, itemId, quantity) =>
        updateCartItemCounts(state, CartState(cartId, itemId, quantity))

      case ShoppingCartEvents.ItemQuantityAdjusted(cartId, itemId, newQuantity) =>
        updateCartItemCounts(state, CartState(cartId, itemId, newQuantity))

      case ShoppingCartEvents.ItemRemoved(cartId, itemId) =>
        val cartItems = state
          .get(cartId)
          .map(_ - itemId)
          .getOrElse(Map.empty)
        Future.successful(state + (cartId -> cartItems))
    }
  }

  private def updateCartItemCounts(
      state: Map[String, Map[String, CartState]],
      cartState: CartState): Future[Map[String, Map[String, CartState]]] = {
    import cartState._
    val f = repo.updateCartState(cartState)
    val itemCount = itemId -> cartState
    val updatedCartItems = state
      .get(cartId)
      .map(_ + itemCount)
      .getOrElse(Map(itemCount))

    f.map(_ => state + (cartId -> updatedCartItems))
  }

  val CheckoutFormat = "%-12s%-9s%-20s%s"

  private def dailyCheckouts(checkouts: Seq[Checkout]): String = {
    if (checkouts.isEmpty) "(no checkouts)"
    else {
      val header = CheckoutFormat.format("Date", "Cart ID", "Item ID", "Quantity")
      val lines = header +: checkouts.map {
          case Checkout(date, cartId, itemId, quantity) => CheckoutFormat.format(date, cartId, itemId, quantity)
        }
      lines.mkString("\n", "\n", "\n")
    }
  }

}
//#guideProjectionHandler
