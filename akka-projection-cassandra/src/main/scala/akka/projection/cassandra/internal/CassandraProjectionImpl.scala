/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

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
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.stream.KillSwitches
import akka.stream.alpakka.cassandra.scaladsl.CassandraSessionRegistry
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi private[akka] object CassandraProjectionImpl {
  sealed trait Strategy
  case object AtMostOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Int, orAfterDuration: FiniteDuration) extends Strategy
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraProjectionImpl[Offset, Envelope](
    override val projectionId: ProjectionId,
    sourceProvider: Option[Offset] => Source[Envelope, _],
    offsetExtractor: Envelope => Offset,
    strategy: CassandraProjectionImpl.Strategy,
    handler: Envelope => Future[Done])
    extends Projection[Envelope] {
  import CassandraProjectionImpl._

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
    val system: ActorSystem = systemProvider.classicSystem
    // FIXME maybe use the session-dispatcher config
    implicit val ec: ExecutionContext = system.dispatcher

    // FIXME make the sessionConfigPath configurable so that it can use same session as akka.persistence.cassandra or alpakka.cassandra
    val sessionConfigPath = "akka.projection.cassandra"
    // FIXME session look could be moved to CassandraOffsetStore if that's better
    val session = CassandraSessionRegistry(system).sessionFor(sessionConfigPath)

    val offsetStore = new CassandraOffsetStore(session)

    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val source: Source[(Offset, Envelope), NotUsed] =
      Source
        .futureSource(lastKnownOffset.map(sourceProvider))
        .via(killSwitch.flow)
        .map(envelope => offsetExtractor(envelope) -> envelope)
        .mapMaterializedValue(_ => NotUsed)

    val handlerFlow: Flow[(Offset, Envelope), Offset, NotUsed] =
      Flow[(Offset, Envelope)].mapAsync(parallelism = 1) {
        case (offset, envelope) => handler(envelope).map(_ => offset)
      }

    val composedSource: Source[Done, NotUsed] = strategy match {
      case AtLeastOnce(1, _) =>
        source.via(handlerFlow).mapAsync(1) { offset => offsetStore.saveOffset(projectionId, offset) }
      case AtLeastOnce(afterEnvelopes, orAfterDuration) =>
        source
          .via(handlerFlow)
          .groupedWithin(afterEnvelopes, orAfterDuration)
          .collect { case grouped if grouped.nonEmpty => grouped.last }
          .mapAsync(parallelism = 1) { offset => offsetStore.saveOffset(projectionId, offset) }
      case AtMostOnce =>
        source
          .mapAsync(parallelism = 1) {
            case (offset, envelope) => offsetStore.saveOffset(projectionId, offset).map(_ => offset -> envelope)
          }
          .via(handlerFlow)
          .map(_ => Done)
    }

    composedSource
  }

  // FIXME
  def processEnvelope(envelope: Envelope)(implicit ec: ExecutionContext): Future[Done] = ???
}
