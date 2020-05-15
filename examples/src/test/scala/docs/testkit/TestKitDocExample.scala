/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.testkit

import scala.concurrent.Future

import akka.Done
import akka.projection.ProjectionId
import akka.projection.slick.SlickProjection

//#testkit-import
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.testkit.scaladsl.ProjectionTestKit

//#testkit-import

//#testkit-duration
import scala.concurrent.duration._

//#testkit-duration

// FIXME: this test can't run, most of its building blocks are 'fake' or are using null values
// abstract so it doesn run for now
abstract
//#testkit
class TestKitDocExample extends ScalaTestWithActorTestKit {
  val projectionTestKit = ProjectionTestKit(testKit)

  //#testkit

  case class CartView(id: String)

  class CartViewRepository {
    def findById(id: String): Future[CartView] = Future.successful(CartView(id))
  }

  val cartViewRepository = new CartViewRepository

  // it only needs to compile
  val projection = SlickProjection.exactlyOnce(
    ProjectionId("test", "00"),
    sourceProvider = null,
    databaseConfig = null,
    handler = null)

  {
    //#testkit-run
    projectionTestKit.run(projection) {
      // confirm that cart checkout was inserted in db
      cartViewRepository.findById("abc-def").futureValue
    }
    //#testkit-run
  }

  {
    //#testkit-run-max-interval
    projectionTestKit.run(projection, max = 5.seconds, interval = 300.millis) {
      // confirm that cart checkout was inserted in db
      cartViewRepository.findById("abc-def").futureValue
    }
    //#testkit-run-max-interval
  }

  //#testkit-sink-probe
  projectionTestKit.runWithTestSink(projection) { sinkProbe =>
    sinkProbe.request(1)
    sinkProbe.expectNext(Done)
  }

  // confirm that cart checkout was inserted in db
  cartViewRepository.findById("abc-def").futureValue

  //#testkit-sink-probe
//#testkit
}
//#testkit
