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
import akka.projection.scaladsl.Handler
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
  def update(date: Instant, itemId: String, quantity: Int): Future[Done]
  def countForDate(date: Instant): Future[Map[String, Int]]
}

class DailyCheckoutProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends DailyCheckoutProjectionRepository {
  val keyspace = "akka_projection"
  val table = "daily_item_checkout_counts"

  def update(date: Instant, itemId: String, quantity: Int): Future[Done] = {
    session.executeWrite(
      s"UPDATE $keyspace.$table SET checkout_count = checkout_count + ? WHERE date = ? AND item_id = ?",
      quantity.toString,
      date,
      itemId)
  }

  def countForDate(date: Instant): Future[Map[String, Int]] = {
    session
      .selectAll(s"SELECT item_id, checkout_count FROM $keyspace.$table WHERE date = ?", date)
      .map { rows =>
        rows.map(row => row.getString("item_id") -> row.getInt("checkout_count")).toMap
      }
  }
}
//#guideProjectionRepo

//#guideProjectionHandler
class DailyCheckoutProjectionHandler(tag: String, system: ActorSystem[_], repo: DailyCheckoutProjectionRepository)
    extends Handler[EventEnvelope[ShoppingCartEvents.Event]] {
  @volatile var checkoutCounter = 0
  @volatile var cartItemCounts = Map[String, Map[String, Int]]()

  val log = LoggerFactory.getLogger(getClass)
  implicit val ec = system.classicSystem.dispatcher

  def updateCartItemCounts(cartId: String, itemId: String, quantity: Int): Unit = {
    val itemCount = (itemId -> quantity)
    val cartItems = cartItemCounts
      .get(cartId)
      .map(_ + itemCount)
      .getOrElse(Map(itemCount))

    cartItemCounts = cartItemCounts + (cartId -> cartItems)
  }

  override def process(envelope: EventEnvelope[ShoppingCartEvents.Event]): Future[Done] = {
    envelope.event match {
      case ShoppingCartEvents.CheckedOut(cartId, eventTime) =>
        val dateOnly = eventTime.truncatedTo(ChronoUnit.DAYS)
        val cartItems = cartItemCounts.getOrElse(cartId, Map.empty)

        cartItemCounts = cartItemCounts - cartId

        val updates = Future.sequence(cartItems.map {
          case (itemId, quantity) => repo.update(dateOnly, itemId, quantity)
        })

        checkoutCounter += 1
        if (checkoutCounter % 10 == 0) {
          updates.flatMap { _ =>
            val countQuery = repo.countForDate(dateOnly)
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
            countQuery.map(_ => Done)
          }
        } else updates.map(_ => Done)

      case ShoppingCartEvents.ItemAdded(cartId, itemId, quantity) =>
        updateCartItemCounts(cartId, itemId, quantity)
        Future.successful(Done)

      case ShoppingCartEvents.ItemQuantityAdjusted(cartId, itemId, newQuantity) =>
        updateCartItemCounts(cartId, itemId, newQuantity)
        Future.successful(Done)

      case ShoppingCartEvents.ItemRemoved(cartId, itemId) =>
        val cartItems = cartItemCounts
          .get(cartId)
          .map(_ - itemId)
          .getOrElse(Map.empty)
        cartItemCounts = cartItemCounts + (cartId -> cartItems)
        Future.successful(Done)
    }
  }
}
//#guideProjectionHandler
