/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.internal.PoisonPill
import akka.actor.typed.internal.PoisonPillInterceptor
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.SpawnExtension
import akka.actor.typed.scaladsl.WatchExtension
import akka.annotation.InternalApi
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ActorHandlerImpl[Envelope, EnvelopeMessage](
    projectionId: ProjectionId,
    behavior: Behavior[_ >: EnvelopeMessage],
    envelopeMessage: (Envelope, ActorRef[Try[Done]]) => EnvelopeMessage)(implicit system: ActorSystem[_])
    extends Handler[Envelope] {
  import system.executionContext
  private implicit val timeout: Timeout = 10.seconds // FIXME config

  // FIXME do we also need a `start` hook? The actor could be spawned from that instead of lazy on
  // first element. Feels wrong to spawn it from constructor before projection is running.

  private lazy val processor: Future[ActorRef[EnvelopeMessage]] =
    SpawnExtension(system).spawn(
      Behaviors
        .intercept(() => new PoisonPillInterceptor[EnvelopeMessage])(behavior.narrow),
      s"projectionHandler-${projectionId.id}")
  private val terminated = processor.flatMap(ref => WatchExtension(system).watch(ref))

  override def process(envelope: Envelope): Future[Done] = {
    processor.flatMap(_.ask[Try[Done]](replyTo => envelopeMessage(envelope, replyTo))).map {
      case Success(_)   => Done
      case Failure(exc) => throw exc
    }
  }

  override def stop(): Future[Done] = {
    // FIXME add possibility to use custom stop message, but can default to PoisonPill
    processor.foreach(_.unsafeUpcast[Any] ! PoisonPill) // signal handled by PoisonPillInterceptor
    terminated
  }
}
