/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.guide

//#guideSetup

import java.time.Instant

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors

//#guideSetup
//#guideSourceProviderImports

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import akka.projection.EventEnvelope
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
  sealed trait Event {
    def cartId: String
  }

  final case class ItemAdded(cartId: String, itemId: String, quantity: Int) extends Event
  final case class ItemRemoved(cartId: String, itemId: String) extends Event
  final case class ItemQuantityAdjusted(cartId: String, itemId: String, newQuantity: Int) extends Event
  final case class CheckedOut(cartId: String, eventTime: Instant) extends Event
}

object ShoppingCartApp extends App {
  private val system = ActorSystem[Nothing](Behaviors.empty, "ShoppingCartApp")
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
  def save(cartId: String, date: Instant): Future[Done]
  def countForDate(date: Instant): Future[Int]
}

class DailyCheckoutProjectionRepositoryImpl(session: CassandraSession)(implicit val ec: ExecutionContext)
    extends DailyCheckoutProjectionRepository {
  val keyspace = "projections"
  val table = "daily_checkouts"

  def save(cartId: String, date: Instant): Future[Done] = {
    session.executeWrite(s"INSERT INTO $keyspace.$table (cart_id, date) VALUES (?, ?)", cartId, date)
  }

  def countForDate(date: Instant): Future[Int] = {
    session.selectOne(s"SELECT count(card_id) FROM $keyspace.$table WHERE date = ?", date).map {
      case Some(row) => row.getInt("count(cart_id)")
      case None      => 0
    }
  }

  //#guideProjectionRepo
  def createKeyspaceAndTable(): Future[Done] = {
    session
      .executeDDL(
        s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH REPLICATION = { 'class' : 'SimpleStrategy','replication_factor':1 }")
      .flatMap(_ => session.executeDDL(s"""
                                          |CREATE TABLE IF NOT EXISTS $keyspace.$table (
                                          |  cart_id string,
                                          |  date date,
                                          |  PRIMARY KEY (cart_id, date))
        """.stripMargin.trim))
  }
  //#guideProjectionRepo
}
//#guideProjectionRepo

//#guideProjectionHandler
class DailyCheckoutProjectionHandler(tag: String, system: ActorSystem[_], repo: DailyCheckoutProjectionRepository)
    extends Handler[EventEnvelope[ShoppingCartEvents.Event]] {
  @volatile var currentDayLogCounter = 0
  val log = LoggerFactory.getLogger(getClass)
  implicit val ec = system.classicSystem.dispatcher

  override def process(envelope: EventEnvelope[ShoppingCartEvents.Event]): Future[Done] = {
    envelope.event match {
      case ShoppingCartEvents.CheckedOut(cartId, eventTime) =>
        val dateOnly = eventTime.truncatedTo(ChronoUnit.DAYS)

        val save = repo.save(cartId, dateOnly)

        currentDayLogCounter += 1
        if (currentDayLogCounter % 10 == 0) {
          save.flatMap { _ =>
            val countQuery = repo.countForDate(dateOnly)
            countQuery.onComplete {
              case Success(count) =>
                log.info(
                  "ShoppingCartProjectionHandler({}) current daily count for today [{}] is {}",
                  tag,
                  dateOnly,
                  count.toString)
              case Failure(ex) =>
                log.error(s"ShoppingCartProjectionHandler($tag) an error occurred when connecting to the repo", ex)
            }
            countQuery.map(_ => Done)
          }
        } else save

      // skip all other shopping cart events
      case _ => Future.successful(Done)
    }
  }
}
//#guideProjectionHandler
