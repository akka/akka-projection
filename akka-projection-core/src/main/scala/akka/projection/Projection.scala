/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange

import scala.concurrent.{ ExecutionContext, Future }

@ApiMayChange
trait Projection[StreamElement] {

  /**
   * The projection group name. The name must be unique over the whole system.
   * For instance, a 'user-view' could be the name of a projection.
   */
  def groupName: String

  /**
   * The unique key for this Projection instance. Must be unique within the projection group.
   */
  def key: String

  /**
   * The method wrapping the user EventHandler function.
   */
  def processElement(elt: StreamElement): Future[StreamElement]

  /**
   * Start to run a Projection
   *
   * @param systemProvider
   */
  def start()(implicit systemProvider: ClassicActorSystemProvider): Unit

  /**
   * Stops the projection is it's running.
   *
   * @return
   */
  def stop(): Future[Done]
}
