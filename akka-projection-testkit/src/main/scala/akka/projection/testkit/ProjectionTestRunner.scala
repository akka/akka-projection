/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.Done
import akka.projection.scaladsl.Projection
import akka.stream.Materializer
import scala.concurrent.duration._
import scala.concurrent.{ Await, ExecutionContext, Future }

trait ProjectionTestRunner {

  def runProjection[Envelope, Event, Offset, Result](proj: Projection[Envelope, Event, Offset, Result])(
      testFunc: => Unit)(implicit ex: ExecutionContext, materializer: Materializer): Unit =
    runProjection(proj, 5.seconds)(testFunc)

  def runProjection[Envelope, Event, Offset, Result](
      proj: Projection[Envelope, Event, Offset, Result],
      timeout: FiniteDuration)(testFunc: => Unit)(implicit ex: ExecutionContext, materializer: Materializer): Unit = {
    try {
      proj.start()
      testFunc
    } finally {
      Await.ready(proj.stop(), timeout)
    }
  }
}
