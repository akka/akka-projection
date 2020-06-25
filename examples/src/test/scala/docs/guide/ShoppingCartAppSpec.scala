package docs.guide

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.Done
import akka.projection.ProjectionId
import akka.projection.testkit.scaladsl.TestProjection
import akka.projection.testkit.scaladsl.TestSourceProvider
import akka.testkit.EventFilter
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

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

  "The DailyCheckoutProjectionHandler" should {
    "only count CheckOut events" in {
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      val events = List[(Long, ShoppingCartEvents.Event)](
        0L -> ShoppingCartEvents.ItemAdded("a70989d4", "batteries", 1),
        1L -> ShoppingCartEvents.ItemQuantityAdjusted("a70989d4", "batteries", 1), //2007-12-03T10:15:30.00Z
        2L -> ShoppingCartEvents.CheckedOut("a70989d4", Instant.parse("2020-01-01T12:10:00.00Z")),
        3L -> ShoppingCartEvents.ItemAdded("0d12dd9d", "crayons", 1),
        4L -> ShoppingCartEvents.CheckedOut("0d12dd9d", Instant.parse("2020-01-01T08:00:00.00Z")))

      val projectionId = ProjectionId("name", "key")
      val sourceProvider = TestSourceProvider(events)
      val projection = TestProjection[ShoppingCartEvents.Event](system, projectionId, sourceProvider, handler)

      projectionTestKit.runWithTestSink(projection) { testSink =>
        testSink.request(events.length)
        testSink.expectNextN(events.length)
        repo.saveCount.get() shouldBe 2
      }
    }

    "log current daily count every 10 messages" in {
      val repo = new MockDailyCheckoutRepository
      val handler = new DailyCheckoutProjectionHandler("tag", system, repo)

      val events =
        for (i <- 0L to 10L)
          yield (i -> (ShoppingCartEvents.CheckedOut(
            java.util.UUID.randomUUID().toString,
            Instant.parse("2020-01-01T08:00:00.00Z")): ShoppingCartEvents.Event))

      val projectionId = ProjectionId("name", "key")
      val sourceProvider = TestSourceProvider(events.toList)
      val projection = TestProjection[ShoppingCartEvents.Event](system, projectionId, sourceProvider, handler)
      //import scala.concurrent.duration._
      import akka.actor.typed.scaladsl.adapter._
      projectionTestKit.run(projection) {
        // NOTE: will this work with akka typed?
        EventFilter
          .info(
            source = classOf[DailyCheckoutProjectionHandler].getName(),
            message =
              "ShoppingCartProjectionHandler(tag) current daily count for today [2020-01-01T00:00:00Z] is 10 MDC: {}",
            occurrences = 1)
          .intercept {}(system.toClassic)
      }
    }
  }
}
