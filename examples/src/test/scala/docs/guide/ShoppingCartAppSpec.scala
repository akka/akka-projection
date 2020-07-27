/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

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
import docs.guide.CheckoutProjectionHandler._
import org.scalatest.wordspec.AnyWordSpecLike

// #testKitSpec
object ShoppingCartAppSpec {
  // mock out the Cassandra data layer and simulate recording checkouts
  class MockCheckoutRepository extends CheckoutProjectionRepository {
    var checkouts = Seq[Checkout]()

    def updateCart(cartId: String): Future[Done] = Future.successful(Done)
    def checkoutCart(checkout: Checkout): Future[Done] = {
      checkouts = checkouts :+ checkout
      Future.successful(Done)
    }
  }
}

class ShoppingCartAppSpec extends ScalaTestWithActorTestKit() with AnyWordSpecLike {
  import ShoppingCartAppSpec._

  val projectionTestKit = ProjectionTestKit(testKit)

  def createEnvelope(event: ShoppingCartEvents.Event, seqNo: Long, timestamp: Long = 0L) =
    EventEnvelope(Offset.sequence(seqNo), "persistenceId", seqNo, event, timestamp)

  "The CheckoutProjectionHandler" should {
    "process cart checkout cart correctly" in {
      val repo = new MockCheckoutRepository
      val handler = new CheckoutProjectionHandler("tag", system, repo)

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
          projectionId,
          sourceProvider,
          () => handler,
          Some(Offset.sequence(0L)))

      projectionTestKit.run(projection) {
        repo.checkouts shouldBe List(
          Checkout("a7098", Instant.parse("2020-01-01T12:10:00.00Z")),
          Checkout("0d12d", Instant.parse("2020-01-01T08:00:00.00Z")))
      }
    }

    "log cart checkouts for day every 10 checkouts" in {
      val repo = new MockCheckoutRepository
      val handler = new CheckoutProjectionHandler("tag", system, repo)

      // create 10 `ItemAdded` and `CheckedOut` events each for one day
      val events = (0L to 20L by 2).flatMap { i =>
        val cartId = (i / 2).toString
        Seq(
          createEnvelope(ShoppingCartEvents.ItemAdded(cartId, "bowling shoes", 2), i),
          createEnvelope(
            ShoppingCartEvents
              .CheckedOut(cartId, Instant.parse("2020-01-01T08:00:00.00Z")): ShoppingCartEvents.Event,
            i + 1))
      }

      val projectionId = ProjectionId("name", "key")
      val sourceProvider =
        TestSourceProvider[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          events.toList,
          extractOffset = env => env.offset)
      val projection =
        TestProjection[Offset, EventEnvelope[ShoppingCartEvents.Event]](
          projectionId,
          sourceProvider,
          () => handler,
          Some(Offset.sequence(0L)))

      LoggingTestKit
        .info("""CheckoutProjectionHandler(tag) last [10] checkouts: 
                |Cart ID     Event Time
                |0           2020-01-01T08:00:00Z
                |1           2020-01-01T08:00:00Z
                |2           2020-01-01T08:00:00Z
                |3           2020-01-01T08:00:00Z
                |4           2020-01-01T08:00:00Z
                |5           2020-01-01T08:00:00Z
                |6           2020-01-01T08:00:00Z
                |7           2020-01-01T08:00:00Z
                |8           2020-01-01T08:00:00Z
                |9           2020-01-01T08:00:00Z""".stripMargin)
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
