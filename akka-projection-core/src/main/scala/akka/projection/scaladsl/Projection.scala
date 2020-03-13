/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import scala.concurrent.ExecutionContext

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

case class Projection[Envelope, Event, Offset](
    systemProvider: ClassicActorSystemProvider,
    sourceProvider: SourceProvider[Offset, Envelope],
    envelopeExtractor: EnvelopeExtractor[Envelope, Event, Offset],
    handler: ProjectionHandler[Event],
    offsetStore: OffsetStore[Offset],
    offsetStrategy: OffsetStore.Strategy)(implicit ec: ExecutionContext) {

  def start(): Unit = {
    implicit val system: ActorSystem = systemProvider.classicSystem

    val offset = offsetStore.readOffset()

    val source: Source[(Offset, Event), NotUsed] =
      Source
        .futureSource(offset.map(sourceProvider.source))
        .map(env => envelopeExtractor.extractOffset(env) -> envelopeExtractor.extractPayload(env))
        .mapMaterializedValue(_ => NotUsed)

    val eventHandler = handler.eventHandler
    val handlerFlow: Flow[(Offset, Event), Offset, NotUsed] =
      Flow[(Offset, Event)].mapAsync(parallelism = 1) {
        case (offset, event) => eventHandler(event).map(_ => offset)
      }

    (offsetStrategy match {
      case OffsetStore.NoOffsetStorage =>
        source.via(handlerFlow).map(_ => Done)

      case OffsetStore.AtMostOnce =>
        source
          .mapAsync(parallelism = 1) {
            case (offset, event) => offsetStore.saveOffset(offset).map(_ => offset -> event)
          }
          .via(handlerFlow)
          .map(_ => Done)

      case OffsetStore.AtLeastOnce(1, d) =>
        source
          .mapAsync(parallelism = 1) {
            case (offset, event) => offsetStore.saveOffset(offset).map(_ => offset -> event)
          }
          .via(handlerFlow)
          .mapAsync(1) { offset =>
            offsetStore.saveOffset(offset)
          }

      case OffsetStore.AtLeastOnce(n, d) =>
        source
          .via(handlerFlow)
          .groupedWithin(n, d)
          .collect { case grouped if grouped.nonEmpty => grouped.last }
          .mapAsync(parallelism = 1) { offset =>
            offsetStore.saveOffset(offset)
          }

    }).runWith(Sink.ignore)

  }
}
