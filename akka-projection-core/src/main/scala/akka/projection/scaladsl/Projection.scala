/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.ExecutionContext

import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

case class Projection[Envelope, Event, Offset, IO](
    systemProvider: ClassicActorSystemProvider,
    sourceProvider: SourceProvider[Offset, Envelope],
    envelopeExtractor: EnvelopeExtractor[Envelope, Event, Offset],
    runner: ProjectionRunner[Offset, IO],
    handler: ProjectionHandler[Event, IO])(implicit ec: ExecutionContext) {

  def start(): Unit = {
    implicit val system: ActorSystem = systemProvider.classicSystem

    val offset = runner.offsetStore.readOffset()

    val source =
      Source.future(offset.map(sourceProvider.source)).flatMapConcat(identity)

    val src =
      source.mapAsync(1) { envelope =>
        // the runner is responsible for the call to ProjectionHandler
        // so it can define what to do with the Offset: at-least-once, at-most-once, effectively-once
        runner.run(envelopeExtractor.extractOffset(envelope)) { () =>
          handler.onEvent(envelopeExtractor.extractPayload(envelope))
        }
      }

    src.runWith(Sink.ignore)
  }
}
