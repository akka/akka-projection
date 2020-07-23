/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

//#guideSetup

import java.time.Instant

import scala.collection.immutable

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import com.typesafe.config.ConfigFactory

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

import akka.Done
import akka.projection.ProjectionId
import akka.projection.cassandra.scaladsl.CassandraProjection
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
      val shoppingCartsTag = "shopping-cart"
      val sourceProvider: SourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]] =
        EventSourcedProvider
          .eventsByTag[ShoppingCartEvents.Event](
            system,
            readJournalPluginId = CassandraReadJournal.Identifier,
            tag = shoppingCartsTag)
      //#guideSourceProviderSetup

      //#guideProjectionSetup
      // FIXME: use a custom dispatcher for repo?
      implicit val ec = system.classicSystem.dispatcher
      val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
      val repo = new CheckoutProjectionRepositoryImpl(session)
      val projection = CassandraProjection.atLeastOnce(
        projectionId = ProjectionId("shopping-carts", shoppingCartsTag),
        sourceProvider,
        handler = () => new CheckoutProjectionHandler(shoppingCartsTag, system, repo))

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
trait CheckoutProjectionRepository {
  import CheckoutProjectionHandler._

  def updateCart(cartId: String): Future[Done]
  def checkoutCart(checkout: Checkout): Future[Done]
}

class CheckoutProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends CheckoutProjectionRepository {
  import CheckoutProjectionHandler._

  val keyspace = "akka_projection"
  val cartStateTable = "cart_checkout_state"

  def updateCart(cartId: String): Future[Done] = {
    session.executeWrite(
      s"UPDATE $keyspace.$cartStateTable SET last_updated = ? WHERE cart_id = ?",
      Instant.now(),
      cartId)
  }

  def checkoutCart(checkout: Checkout): Future[Done] = {
    session.executeWrite(
      s"UPDATE $keyspace.$cartStateTable SET last_updated = ?, checkout_time = ? WHERE cart_id = ?",
      Instant.now(),
      checkout.checkoutTime,
      checkout.cartId)
  }
}
//#guideProjectionRepo

//#guideProjectionHandler
object CheckoutProjectionHandler {
  val CheckoutLogInterval = 10
  val CheckoutFormat = "%-12s%-9s"

  final case class Checkout(cartId: String, checkoutTime: Instant)
}

class CheckoutProjectionHandler(tag: String, system: ActorSystem[_], repo: CheckoutProjectionRepository)
    extends Handler[EventEnvelope[ShoppingCartEvents.Event]]() {

  import CheckoutProjectionHandler._

  @volatile private var checkouts: immutable.Seq[Checkout] = Nil
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.classicSystem.dispatcher

  override def process(envelope: EventEnvelope[ShoppingCartEvents.Event]): Future[Done] = {
    envelope.event match {
      case ShoppingCartEvents.CheckedOut(cartId, eventTime) =>
        val checkout = Checkout(cartId, eventTime)
        val update = repo.checkoutCart(checkout)

        checkouts = checkouts :+ checkout
        if (checkouts.length == CheckoutLogInterval) {
          log.info(
            "CheckoutProjectionHandler({}) last [{}] checkouts: {}",
            tag,
            CheckoutLogInterval.toString,
            formatCheckouts(checkouts))
          checkouts = Nil
        }

        update.map(_ => Done)
      case ShoppingCartEvents.ItemAdded(cartId, _, _) =>
        repo.updateCart(cartId)

      case ShoppingCartEvents.ItemQuantityAdjusted(cartId, _, _) =>
        repo.updateCart(cartId)

      case ShoppingCartEvents.ItemRemoved(cartId, _) =>
        repo.updateCart(cartId)
    }
  }

  private def formatCheckouts(checkouts: immutable.Seq[Checkout]): String = {
    if (checkouts.isEmpty) "(no checkouts)"
    else {
      val header = CheckoutFormat.format("Cart ID", "Event Time")
      val lines = header +: checkouts.map {
          case Checkout(cartId, checkoutTime) => CheckoutFormat.format(cartId, checkoutTime)
        }
      lines.mkString("\n", "\n", "\n")
    }
  }

}
//#guideProjectionHandler
