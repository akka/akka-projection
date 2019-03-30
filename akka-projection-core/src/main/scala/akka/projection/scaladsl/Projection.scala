/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}

import scala.concurrent.ExecutionContext

case class Projection[Envelope, Event, Offset, IO](name: String,
                                                   sourceProvider: SourceProvider[Envelope, Event, Offset],
                                                   offsetManagement: OffsetManagement[Offset, IO],
                                                   handler: ProjectionHandler[Event, IO]) {

  def start(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val offset = offsetManagement.readOffset(name)

    val source =
      Source
        .fromFuture(offset.map(sourceProvider.source))
        .flatMapConcat(identity)

    val src =
      source.mapAsync(1) { envelope =>
        // OffsetManagement is responsible for the call to ProjectionHandler
        // so it can define what to do with the Offset: at-least-once, at-most-once, effectively-once
        offsetManagement.run(name, sourceProvider.extractOffset(envelope))  {
          handler.onEvent(sourceProvider.extractPayload(envelope))
        }
      }

    src.runWith(Sink.ignore)
  }
}
