/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.SendProducer
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.internal.OffsetStore
import akka.projection.scaladsl.SourceProvider
import akka.stream.KillSwitches
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KafkaProjectionImpl {
  sealed trait Strategy
  case object AtMostOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Int, orAfterDuration: FiniteDuration) extends Strategy
  // TODO: transactional, add akka-projection issue link
  //final case class ExactlyOnce(orAfterDuration: FiniteDuration) extends Strategy
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class KafkaProjectionImpl[Offset, Envelope, Key, Value](
    override val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    offsetStore: OffsetStore[Offset, Future], // Kafka doesn't have its own OffsetStore yet
    settings: ProducerSettings[Key, Value],
    strategy: KafkaProjectionImpl.Strategy,
    handler: Envelope => ProducerRecord[Key, Value])
    extends Projection[Envelope] {
  import KafkaProjectionImpl._

  private val killSwitch = KillSwitches.shared(projectionId.id)
  private val promiseToStop: Promise[Done] = Promise()
  private val started = new AtomicBoolean(false)

  override def run()(implicit systemProvider: ClassicActorSystemProvider): Unit = {
    if (started.compareAndSet(false, true)) {
      implicit val system: ActorSystem = systemProvider.classicSystem

      val done = mappedSource().runWith(Sink.ignore)
      promiseToStop.completeWith(done)
    }
  }

  override def stop()(implicit ec: ExecutionContext): Future[Done] = {
    if (started.get()) {
      killSwitch.shutdown()
      promiseToStop.future
    } else {
      Future.failed(new IllegalStateException(s"Projection [$projectionId] not started yet!"))
    }
  }

  override private[projection] def mappedSource()(
      implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] = {
    implicit val ec = systemProvider.classicSystem.dispatcher

    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val source: Source[(Offset, Envelope), NotUsed] =
      Source
        .futureSource(lastKnownOffset.map(sourceProvider.source))
        .via(killSwitch.flow)
        .map(envelope => sourceProvider.extractOffset(envelope) -> envelope)
        .mapMaterializedValue(_ => NotUsed)

    val sendProducer = SendProducer(settings)(systemProvider.classicSystem)

    val handlerFlow: Flow[(Offset, Envelope), Offset, NotUsed] =
      Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
        case (offset, envelope) =>
          val pr = handler(envelope)
          val metadata: Future[RecordMetadata] = sendProducer.send(pr)
          metadata.map(_ => offset)
      }

    val composedSource: Source[Done, NotUsed] = strategy match {
      case AtLeastOnce(1, _) =>
        source
          .via(handlerFlow)
          .mapAsync(1) { offset => offsetStore.saveOffsetAsync(projectionId, offset) }
      case AtLeastOnce(afterEnvelopes, orAfterDuration) =>
        source
          .via(handlerFlow)
          .groupedWithin(afterEnvelopes, orAfterDuration)
          .collect { case grouped if grouped.nonEmpty => grouped.last }
          .mapAsync(parallelism = 1) { offset => offsetStore.saveOffsetAsync(projectionId, offset) }
      case AtMostOnce =>
        source
          .mapAsync(parallelism = 1) {
            case (offset, envelope) => offsetStore.saveOffsetAsync(projectionId, offset).map(_ => offset -> envelope)
          }
          .via(handlerFlow)
          .map(_ => Done)
    }

    composedSource
  }
}
