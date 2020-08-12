/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

//#guideSetup

import java.time.Instant

import scala.util.Success

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.EventEnvelope
import akka.projection.scaladsl.Handler
import com.typesafe.config.ConfigFactory
import docs.guide.ShoppingCartEvents.ItemEvent

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

  sealed trait ItemEvent extends Event {
    def itemId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends ItemEvent
  final case class ItemRemoved(cartId: String, itemId: String, oldQuantity: Int) extends ItemEvent
  final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int, oldQuantity: Int)
      extends ItemEvent
  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event
}

object ShoppingCartApp extends App {
  val config = ConfigFactory.load("guide-shopping-cart-app.conf")

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
      implicit val ec = system.executionContext
      val session = CassandraSessionRegistry(system).sessionFor("akka.projection.cassandra.session-config")
      val repo = new ItemPopularityProjectionRepositoryImpl(session)
      val projection = CassandraProjection.atLeastOnce(
        projectionId = ProjectionId("shopping-carts", shoppingCartsTag),
        sourceProvider,
        handler = () => new ItemPopularityProjectionHandler(shoppingCartsTag, system, repo))

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
trait ItemPopularityProjectionRepository {

  def update(itemId: String, delta: Int): Future[Done]
  def getItem(itemId: String): Future[Option[(String, Long)]]
}

class ItemPopularityProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends ItemPopularityProjectionRepository {

  val keyspace = "akka_projection"
  val popularityTable = "item_popularity"

  def update(itemId: String, delta: Int): Future[Done] = {
    session.executeWrite(
      s"UPDATE $keyspace.$popularityTable SET count = count + ? WHERE item_id = ?",
      java.lang.Long.valueOf(delta),
      itemId)
  }

  def getItem(itemId: String): Future[Option[(String, Long)]] = {
    session
      .selectOne(s"SELECT item_id, count FROM $keyspace.$popularityTable WHERE item_id = ?", itemId)
      .map(opt => opt.map(row => (row.getString("item_id"), row.getLong("count").longValue())))
  }
}
//#guideProjectionRepo

//#guideProjectionHandler
object ItemPopularityProjectionHandler {
  val LogInterval = 10
}

class ItemPopularityProjectionHandler(tag: String, system: ActorSystem[_], repo: ItemPopularityProjectionRepository)
    extends Handler[EventEnvelope[ShoppingCartEvents.Event]]() {

  private var logCounter: Int = 0
  private val log = LoggerFactory.getLogger(getClass)
  private implicit val ec: ExecutionContext = system.executionContext

  override def process(envelope: EventEnvelope[ShoppingCartEvents.Event]): Future[Done] = {
    logItemCount(envelope.event)
    val processed = envelope.event match {
      case ShoppingCartEvents.ItemAdded(_, itemId, quantity) =>
        repo.update(itemId, quantity)

      case ShoppingCartEvents.ItemQuantityAdjusted(_, itemId, newQuantity, oldQuantity) =>
        repo.update(itemId, newQuantity - oldQuantity)

      case ShoppingCartEvents.ItemRemoved(_, itemId, oldQuantity) =>
        repo.update(itemId, 0 - oldQuantity)

      case _: ShoppingCartEvents.CheckedOut => Future.successful(Done)
    }
    processed.onComplete {
      case Success(_) => logItemCount(envelope.event)
      case _          => ()
    }
    processed
  }

  private def logItemCount(event: ShoppingCartEvents.Event): Unit = event match {
    case itemEvent: ItemEvent =>
      logCounter += 1
      val itemId = itemEvent.itemId
      if (logCounter == ItemPopularityProjectionHandler.LogInterval) {
        logCounter = 0
        repo.getItem(itemId).foreach {
          case Some((itemId, count)) =>
            log.info("ItemPopularityProjectionHandler({}) item popularity for '{}': [{}]", tag, itemId, count.toString)
          case None =>
            log.info("ItemPopularityProjectionHandler({}) item popularity for '{}': [0]", tag, itemId)
        }
      }
    case _ => ()
  }

}
//#guideProjectionHandler
