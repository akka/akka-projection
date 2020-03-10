/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.{KillSwitch, KillSwitches, Materializer}
import akka.stream.scaladsl.{Keep, Sink, Source}

import scala.concurrent.ExecutionContext

case class Projection[Envelope, Event, Offset, Result](sourceProvider: SourceProvider[Offset, Envelope],
                                                       envelopeExtractor: EnvelopeExtractor[Envelope, Event, Offset],
                                                       runner: ProjectionRunner[Offset, Result],
                                                       handler: EventHandler[Event, Result]) {


  private var shutdown: Option[KillSwitch] = None

  def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val offsetFut = runner.offsetStore.readOffset()

    val source =
      Source
        .fromFuture(offsetFut.map(sourceProvider.source))
        .flatMapConcat(identity)

    val src =
      // TODO: runner could return a Flow that defines the mapAsync 
      // so different implementations could decide if it makes sense or not
      source.mapAsync(1) { envelope =>
        // the runner is responsible for the call to EventHandler
        // so it can define what to do with the Offset: at-least-once, at-most-once, effectively-once
        runner.run(envelopeExtractor.extractOffset(envelope))  { () =>
          handler.onEvent(envelopeExtractor.extractPayload(envelope))
        }
      }


    val (killSwitch, streamDone) = src
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.ignore)(Keep.both)
      .run()

    shutdown = Some(killSwitch)

  }

  def stop(): Unit = shutdown.foreach( _.shutdown() )
}
