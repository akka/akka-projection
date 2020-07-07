package docs.guide

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import akka.Done
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.persistence.query.Offset
import akka.projection.ProjectionId
import akka.projection.eventsourced.EventEnvelope
// #testKitImports
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.TestProjection
import akka.projection.testkit.TestSourceProvider
// #testKitImports
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

// #testKitSpec
object ShoppingCartAppSpec {
  class MockDailyCheckoutRepository extends DailyCheckoutProjectionRepository {
    val saveCount = new AtomicInteger()
    override def save(cartId: String, date: Instant): Future[Done] = {
      saveCount.incrementAndGet()
      Future.successful(Done)
    }
    override def countForDate(date: Instant): Future[Int] = Future.successful(saveCount.get())
  }
}

class ShoppingCartAppSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.parseString("""
    |akka.loggers = ["akka.testkit.TestEventListener"]
    |""".stripMargin))
    with AnyWordSpecLike {
  import ShoppingCartAppSpec._

  val projectionTestKit = ProjectionTestKit(testKit)

  def createEnvelope(event: ShoppingCartEvents.Event, seqNo: Long, timestamp: Long = 0L) =
    EventEnvelope(Offset.sequence(seqNo), "persistenceId", seqNo, event, timestamp)

  "The DailyCheckoutProjectionHandler" should {
    "only count CheckOut events" in {
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      val events = List[EventEnvelope[ShoppingCartEvents.Event]](
        createEnvelope(ShoppingCartEvents.ItemAdded("a70989d4", "batteries", 1), 0L),
        createEnvelope(ShoppingCartEvents.ItemQuantityAdjusted("a70989d4", "batteries", 1), 1L),
        createEnvelope(ShoppingCartEvents.CheckedOut("a70989d4", Instant.parse("2020-01-01T12:10:00.00Z")), 2L),
        createEnvelope(ShoppingCartEvents.ItemAdded("0d12dd9d", "crayons", 1), 3L),
        createEnvelope(ShoppingCartEvents.CheckedOut("0d12dd9d", Instant.parse("2020-01-01T08:00:00.00Z")), 4L))

      val projectionId = ProjectionId("name", "key")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]](events, extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](system, projectionId, sourceProvider, handler)

      projectionTestKit.runWithTestSink(projection) { testSink =>
        testSink.request(events.length)
        testSink.expectNextN(events.length)
        repo.saveCount.get() shouldBe 2
      }
    }

    "log current daily count every 10 messages" in {
      val numEvents = 10L
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      val events =
        for (i <- 0L to numEvents)
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
            testSink.request(numEvents)
            testSink.expectNextN(numEvents)
          }
        }
    }
  }
}
// #testKitSpec
