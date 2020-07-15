/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

//#guideSetup

import java.time.Instant

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.eventsourced.EventEnvelope
import com.typesafe.config.ConfigFactory
import docs.guide.DailyCheckoutProjectionHandler.CartState
import docs.guide.DailyCheckoutProjectionHandler.DailyCheckoutItemCount

//#guideSetup
//#guideSourceProviderImports

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.scaladsl.SourceProvider

//#guideSourceProviderImports

//#guideProjectionImports

import java.time.temporal.ChronoUnit

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Success
import scala.util.Failure

import akka.Done
import akka.projection.ProjectionId
import akka.projection.ProjectionBehavior
import akka.projection.scaladsl.StatefulHandler
import akka.projection.cassandra.scaladsl.CassandraProjection
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.alpakka.cassandra.scaladsl.CassandraSession
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
  val config = ConfigFactory.parseResources("./guide-shopping-cart-app.conf")
  val system = ActorSystem[Nothing](Behaviors.empty, "ShoppingCartApp", config)
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
  //#guideSetup

  //#guideProjectionSetup
  // FIXME: use a custom dispatcher for repo?
  implicit val ec = system.classicSystem.dispatcher
  val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
  val repo = new DailyCheckoutProjectionRepositoryImpl(session)
  val projection = CassandraProjection.atLeastOnce(
    projectionId = ProjectionId("shopping-carts", europeanShoppingCartsTag),
    sourceProvider,
    handler = () => new DailyCheckoutProjectionHandler(europeanShoppingCartsTag, system, repo))

  Behaviors.setup[Nothing] { context =>
    context.spawn(ProjectionBehavior(projection), "daily-count-projection")
    Behaviors.empty
  }
  //#guideProjectionSetup
}
//#guideSetup

//#guideProjectionRepo
trait DailyCheckoutProjectionRepository {
  import DailyCheckoutProjectionHandler._

  def checkoutCountsForDate(date: Instant): Future[Seq[DailyCheckoutItemCount]]
  def updateCheckoutItemCount(cartId: String, checkoutItem: DailyCheckoutItemCount): Future[Done]
  def cartState(): Future[Seq[CartState]]
  def updateCartState(cartState: CartState): Future[Done]
  def deleteCartState(cartId: String): Future[Done]
}

class DailyCheckoutProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends DailyCheckoutProjectionRepository {
  import DailyCheckoutProjectionHandler._

  val keyspace = "akka_projection"
  val dailyItemCheckoutCountsTable = "daily_item_checkout_counts"
  val cartStateTable = "cart_state"

  def checkoutCountsForDate(date: Instant): Future[Seq[DailyCheckoutItemCount]] = {
    session
      .selectAll(s"SELECT item_id, checkout_count FROM $keyspace.$dailyItemCheckoutCountsTable WHERE date = ?", date)
      .map { rows =>
        rows.map(row => DailyCheckoutItemCount(row.getString("item_id"), date, row.getInt("checkout_count")))
      }
  }

  def updateCheckoutItemCount(cartId: String, checkoutItem: DailyCheckoutItemCount): Future[Done] = {
    import checkoutItem._
    session.executeWrite(
      s"UPDATE $keyspace.$dailyItemCheckoutCountsTable SET checkout_count = checkout_count + $count, last_cart_id = ? WHERE date = ? AND item_id = ? AND last_cart_id != ?",
      cartId,
      date,
      itemId,
      cartId)
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
      quantity.toString,
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
  final case class DailyCheckoutItemCount(itemId: String, date: Instant, count: Int)
  final case class CartState(cartId: String, itemId: String, quantity: Int)
}

class DailyCheckoutProjectionHandler(tag: String, system: ActorSystem[_], repo: DailyCheckoutProjectionRepository)
    extends StatefulHandler[Map[String, Map[String, CartState]], EventEnvelope[ShoppingCartEvents.Event]]()(
      system.classicSystem.dispatcher) {

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
        val dateOnly = eventTime.truncatedTo(ChronoUnit.DAYS)
        val cartItems = state.getOrElse(cartId, Map.empty)

        val updates = Future.sequence(cartItems.map {
          case (itemId, cartState) =>
            repo.updateCheckoutItemCount(cartId, DailyCheckoutItemCount(itemId, dateOnly, cartState.quantity))
        })

        checkoutCounter += 1
        val updatesWithCountStatus = if (checkoutCounter % 10 == 0) {
          updates.flatMap { _ =>
            val countQuery = repo.checkoutCountsForDate(dateOnly)
            countQuery.onComplete {
              case Success(counts) =>
                log.info(
                  "DailyCheckoutProjectionHandler({}) current daily item counts for today [{}] is {}",
                  tag,
                  dateOnly,
                  counts.toString)
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

}
//#guideProjectionHandler
