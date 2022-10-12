/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.projection.internal.ActorHandlerInit

/**
 * This [[Handler]] gives support for spawning an actor of a given `Behavior` to delegate
 * processing of the envelopes to the actor.
 *
 * The lifecycle of the actor is managed by the `Projection`. The `behavior` is spawned when the
 * `Projection` is started and the `ActorRef` is passed in as a parameter to the `process` method.
 * The Actor is stopped when the `Projection` is stopped.
 */
abstract class ActorHandler[Envelope, T](val behavior: Behavior[T]) extends Handler[Envelope] with ActorHandlerInit[T] {

  /**
   * The `process` method is invoked for each `Envelope`.
   * One envelope is processed at a time. The returned `Future` is to be completed when the processing
   * of the `envelope` has finished. It will not be invoked with the next envelope until after the returned
   * `Future` has been completed.
   *
   * The `behavior` is spawned when the `Projection` is started and the `ActorRef` is passed in as
   * a parameter here.
   *
   * You will typically use the `AskPattern.ask` to delegate the processing of the `envelope` to
   * the actor and the returned `Future` corresponds to the reply of the `ask`.
   */
  def process(actor: ActorRef[T], envelope: Envelope): Future[Done]

  override final def process(envelope: Envelope): Future[Done] =
    process(getActor(), envelope)

}
