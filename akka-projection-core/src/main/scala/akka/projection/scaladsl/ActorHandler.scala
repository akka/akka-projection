/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.projection.internal.ActorHandlerImpl

abstract class ActorHandler[Envelope, T](behavior: Behavior[T])
    extends ActorHandlerImpl[Envelope, T](behavior)
    with Handler[Envelope] {

  def process(envelope: Envelope, actor: ActorRef[T]): Future[Done]

}
