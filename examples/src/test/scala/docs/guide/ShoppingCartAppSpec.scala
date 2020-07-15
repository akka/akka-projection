package docs.guide

import java.time.Instant

import scala.concurrent.Future

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Offset
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
// #testKitImports
import akka.projection.testkit.TestProjection
import akka.projection.testkit.TestSourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
// #testKitImports
import org.scalatest.wordspec.AnyWordSpecLike

import DailyCheckoutProjectionHandler._

// #testKitSpec
object ShoppingCartAppSpec {
  // manually mock out the Cassandra data layer and simulate recording the daily count
  class MockDailyCheckoutRepository extends DailyCheckoutProjectionRepository {
    var updateCheckoutItemCounts = Seq[DailyCheckoutItemCount]()

    override def checkoutCountsForDate(date: Instant): Future[Seq[DailyCheckoutItemCount]] =
      Future.successful(updateCheckoutItemCounts)
    override def updateCheckoutItemCount(cartId: String, checkoutItem: DailyCheckoutItemCount): Future[Done] = {
      updateCheckoutItemCounts = updateCheckoutItemCounts :+ checkoutItem
      Future.successful(Done)
    }
    override def cartState(): Future[Seq[CartState]] = Future.successful(Seq.empty)
    override def updateCartState(cartState: CartState): Future[Done] = Future.successful(Done)
    override def deleteCartState(cartId: String): Future[Done] = Future.successful(Done)
  }
}

class ShoppingCartAppSpec extends ScalaTestWithActorTestKit() with AnyWordSpecLike {
  import ShoppingCartAppSpec._

  val projectionTestKit = ProjectionTestKit(testKit)

  def createEnvelope(event: ShoppingCartEvents.Event, seqNo: Long, timestamp: Long = 0L) =
    EventEnvelope(Offset.sequence(seqNo), "persistenceId", seqNo, event, timestamp)

  "The DailyCheckoutProjectionHandler" should {
    "process cart checkout cart correctly" in {
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      val events = List[EventEnvelope[ShoppingCartEvents.Event]](
        createEnvelope(ShoppingCartEvents.ItemAdded("a7098", "batteries", 1), 0L),
        createEnvelope(ShoppingCartEvents.ItemQuantityAdjusted("a7098", "batteries", 2), 1L),
        createEnvelope(ShoppingCartEvents.CheckedOut("a7098", Instant.parse("2020-01-01T12:10:00.00Z")), 2L),
        createEnvelope(ShoppingCartEvents.ItemAdded("0d12d", "crayons", 1), 3L),
        createEnvelope(ShoppingCartEvents.ItemAdded("0d12d", "pens", 1), 4L),
        createEnvelope(ShoppingCartEvents.ItemRemoved("0d12d", "pens"), 5L),
        createEnvelope(ShoppingCartEvents.CheckedOut("0d12d", Instant.parse("2020-01-01T08:00:00.00Z")), 6L))

      val projectionId = ProjectionId("name", "key")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]](events, extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          system,
          projectionId,
          sourceProvider,
          Offset.sequence(0L),
          handler)

      val expectedDay = Instant.parse("2020-01-01T00:00:00.00Z")
      projectionTestKit.runWithTestSink(projection) { testSink =>
        testSink.request(events.length)
        testSink.expectNextN(events.length)
        repo.updateCheckoutItemCounts shouldBe List(
          DailyCheckoutItemCount("batteries", expectedDay, 2),
          DailyCheckoutItemCount("crayons", expectedDay, 1))
      }
    }

    "log current daily item counts every 10 checkouts" in {
      val repo = new MockDailyCheckoutRepository {
        override def checkoutCountsForDate(date: Instant): Future[Seq[DailyCheckoutItemCount]] = {
          val count = updateCheckoutItemCounts.foldLeft(0) {
            case (acc, dailyCheckoutItemCount) => acc + dailyCheckoutItemCount.count
          }
          Future.successful(List(updateCheckoutItemCounts.head.copy(count = count)))
        }
      }
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      // create 10 `ItemAdded` and `CheckedOut` events each for one day
      val events = (0L to 20L by 2).flatMap { i =>
        val cartId = java.util.UUID.randomUUID().toString.take(5)
        Seq(
          createEnvelope(ShoppingCartEvents.ItemAdded(cartId, "bowling shoes", 2), i),
          createEnvelope(
            ShoppingCartEvents.CheckedOut(cartId, Instant.parse("2020-01-01T08:00:00.00Z")): ShoppingCartEvents.Event,
            i + 1))
      }

      val projectionId = ProjectionId("name", "key")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          events.toList,
          extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          system,
          projectionId,
          sourceProvider,
          Offset.sequence(0L),
          handler)

      LoggingTestKit
        .info(
          "DailyCheckoutProjectionHandler(tag) current daily item counts for today [2020-01-01T00:00:00Z] is List(DailyCheckoutItemCount(bowling shoes,2020-01-01T00:00:00Z,20))")
        .expect {
          projectionTestKit.runWithTestSink(projection) { testSink =>
            testSink.request(events.length)
            testSink.expectNextN(events.length)
            repo.updateCheckoutItemCounts.foldLeft(0) {
              case (acc, dailyCheckoutItemCount) => acc + dailyCheckoutItemCount.count
            }
          }
        }
    }
  }
}
// #testKitSpec
