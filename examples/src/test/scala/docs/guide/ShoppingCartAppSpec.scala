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

// #testKitSpec
object ShoppingCartAppSpec {
  // manually mock out the Cassandra data layer and simulate recording the daily count
  class MockDailyCheckoutRepository extends DailyCheckoutProjectionRepository {
    var updateCount = Map[String, Int]()
    override def update(date: Instant, itemId: String, quantity: Int): Future[Done] = {
      updateCount = updateCount + (itemId -> quantity)
      Future.successful(Done)
    }
    override def countForDate(date: Instant): Future[Map[String, Int]] = Future.successful(updateCount)
  }
}

class ShoppingCartAppSpec extends ScalaTestWithActorTestKit() with AnyWordSpecLike {
  import ShoppingCartAppSpec._

  val projectionTestKit = ProjectionTestKit(testKit)

  def createEnvelope(event: ShoppingCartEvents.Event, seqNo: Long, timestamp: Long = 0L) =
    EventEnvelope(Offset.sequence(seqNo), "persistenceId", seqNo, event, timestamp)

  "The DailyCheckoutProjectionHandler" should {
    "process shopping cart events correctly" in {
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
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](system, projectionId, sourceProvider, handler)

      projectionTestKit.runWithTestSink(projection) { testSink =>
        testSink.request(events.length)
        testSink.expectNextN(events.length)
        repo.updateCount shouldBe Map("batteries" -> 2, "crayons" -> 1)
      }
    }

    // TODO: update for current daily item count
    "log current daily item count every 10 checkouts" in {
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      // create 10 `CheckedOut` events for the same day
      val events =
        for (i <- 0L to 10L)
          yield createEnvelope(
            ShoppingCartEvents.CheckedOut(
              java.util.UUID.randomUUID().toString,
              Instant.parse("2020-01-01T08:00:00.00Z")): ShoppingCartEvents.Event,
            i)

      val projectionId = ProjectionId("name", "key")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          events.toList,
          extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](system, projectionId, sourceProvider, handler)

      LoggingTestKit
        .info("ShoppingCartProjectionHandler(tag) current daily count for today [2020-01-01T00:00:00Z] is 10")
        .expect {
          projectionTestKit.runWithTestSink(projection) { testSink =>
            testSink.request(events.length)
            testSink.expectNextN(events.length)
          }
        }
    }
  }
}
// #testKitSpec
