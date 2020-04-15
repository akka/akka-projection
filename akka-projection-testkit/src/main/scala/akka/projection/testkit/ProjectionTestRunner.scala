/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.projection.Projection

import scala.concurrent.Await
import scala.concurrent.duration._

@ApiMayChange
trait ProjectionTestRunner {

  def runProjection(proj: Projection[_])(testFunc: => Unit)(implicit systemProvider: ClassicActorSystemProvider): Unit =
    runProjection(proj, 5.seconds)(testFunc)

  def runProjection(proj: Projection[_], timeout: FiniteDuration)(testFunc: => Unit)(
      implicit systemProvider: ClassicActorSystemProvider): Unit = {
    try {
      proj.start()
      testFunc
    } finally {
      Await.ready(proj.stop(), timeout)
    }
  }
}
