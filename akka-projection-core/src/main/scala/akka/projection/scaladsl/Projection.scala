/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.Done
import akka.stream.{ KillSwitch, KillSwitches, Materializer }
import akka.stream.scaladsl.{ Keep, Sink, Source }

import scala.concurrent.{ ExecutionContext, Future }

trait Projection[Envelope, Event, Offset, Result] {
  def processEnvelope(envelope: Envelope)(implicit ex: ExecutionContext): Future[Done]
  def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit
  def stop()(implicit ex: ExecutionContext): Future[Done]
}

case class ProjectionBase[Envelope, Event, Offset, Result](
    sourceProvider: SourceProvider[Offset, Envelope, _],
    envelopeExtractor: EnvelopeExtractor[Envelope, Event, Offset],
    runner: ProjectionRunner[Offset, Result],
    handler: EventHandler[Event, Result])
    extends Projection[Envelope, Event, Offset, Result] {

  private var shutdown: Option[KillSwitch] = None

  def processEnvelope(envelope: Envelope)(implicit ex: ExecutionContext): Future[Done] =
    runner.run(envelopeExtractor.extractOffset(envelope)) { () =>
      handler.onEvent(envelopeExtractor.extractPayload(envelope))
    }

  def start()(implicit ex: ExecutionContext, materializer: Materializer): Unit = {

    val offsetFut = runner.offsetStore.readOffset()

    val (killSwitch, streamDone) =
      Source
        .fromFuture(offsetFut.map(sourceProvider.source))
        .flatMapConcat(identity)
        .mapAsync(1)(processEnvelope)
        .viaMat(KillSwitches.single)(Keep.right)
        .toMat(Sink.ignore)(Keep.both)
        .run()

    shutdown = Some(killSwitch)

  }

  def stop()(implicit ex: ExecutionContext): Future[Done] = {
    shutdown.foreach(_.shutdown())
    Future.successful(Done)
  }
}
