package docs.guide

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.Done
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.projection.testkit.scaladsl.TestProjection
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.projection.EventEnvelope
import org.scalatest.wordspec.AnyWordSpecLike

class ShoppingCartAppSpec extends ScalaTestWithActorTestKit() with AnyWordSpecLike {
  val projectionTestKit = ProjectionTestKit(testKit)

  "The DailyCheckoutProjectionHandler" should {
    "only count CheckOut events" in {
      val saveCount: AtomicInteger = new AtomicInteger()
      val repo = new DailyCheckoutProjectionRepository {
        override def save(cartId: String, date: Instant): Future[Done] = {
          saveCount.incrementAndGet()
          Future.successful(Done)
        }
        override def countForDate(date: Instant): Future[Option[Int]] = ???
      }

      val projectionId = ProjectionId("name", "key")

      val handler: Handler[EventEnvelope[ShoppingCartEvents.Event]] =
        new DailyCheckoutProjectionHandler("tag", system, repo)

      val events = List[(Long, ShoppingCartEvents.Event)](
        0L -> ShoppingCartEvents.ItemAdded("a70989d4", "batteries", 1),
        1L -> ShoppingCartEvents.ItemQuantityAdjusted("a70989d4", "batteries", 1), //2007-12-03T10:15:30.00Z
        2L -> ShoppingCartEvents.CheckedOut("a70989d4", Instant.parse("2020-01-01T12:10:00.00Z")),
        3L -> ShoppingCartEvents.ItemAdded("0d12dd9d", "crayons", 1),
        4L -> ShoppingCartEvents.CheckedOut("0d12dd9d", Instant.parse("2020-01-01T08:00:00.00Z")))

      val sourceProvider = TestSourceProvider(events)
      val projection = TestProjection[ShoppingCartEvents.Event](system, projectionId, sourceProvider, handler)
      projectionTestKit.runWithTestSink(projection) { testSink =>
        testSink.request(events.length)
        testSink.expectNextN(events.length)
        saveCount.get() shouldBe 2
      }
    }
  }
}
