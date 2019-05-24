/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import akka.projection.scaladsl.Projection
import akka.stream.Materializer

import scala.concurrent.ExecutionContext

trait ProjectionTestRunner {

  def runProjection[Envelope, Event, Offset, Result](proj: Projection[Envelope, Event, Offset, Result])
                                                    (testFunc: => Unit)
                                                    (implicit ex: ExecutionContext, materializer: Materializer) = {
    try {
      proj.start()
      testFunc
    } finally
      proj.stop()

  }
}
