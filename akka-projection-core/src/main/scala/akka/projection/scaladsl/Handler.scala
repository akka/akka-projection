/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.Done

object Handler {

  /** Handler that can be define from a simple function */
  private class HandlerFunction[Envelope](handler: Envelope => Future[Done]) extends Handler[Envelope] {
    override def process(envelope: Envelope): Future[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => Future[Done]): Handler[Envelope] = new HandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in the `Projection`. Some projections
 * may have more specific handler types.
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 */
trait Handler[Envelope] {
  def process(envelope: Envelope): Future[Done]
}
