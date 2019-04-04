/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.ExecutionContext

case class Projection[Envelope, Event, Offset, IO](sourceProvider: SourceProvider[Offset, Envelope],
                                                   envelopeExtractor: EnvelopeExtractor[Envelope, Event, Offset],
                                                   runner: ProjectionRunner[Offset, IO],
                                                   handler: ProjectionHandler[Event, IO]) {

  def start(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val offset = runner.offsetStore.readOffset()

    val source =
      Source
        .fromFuture(offset.map(sourceProvider.source))
        .flatMapConcat(identity)

    val src =
      source.mapAsync(1) { envelope =>
        // OffsetManagement is responsible for the call to ProjectionHandler
        // so it can define what to do with the Offset: at-least-once, at-most-once, effectively-once
        runner.run(envelopeExtractor.extractOffset(envelope))  {
          handler.onEvent(envelopeExtractor.extractPayload(envelope))
        }
      }

    src.runWith(Sink.ignore)
  }
}
