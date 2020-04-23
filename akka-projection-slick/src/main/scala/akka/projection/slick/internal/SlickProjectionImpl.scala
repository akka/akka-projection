/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.{ Projection, ProjectionId }
import akka.stream.KillSwitches
import akka.stream.scaladsl.{ Flow, Sink, Source }
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile
import scala.concurrent.{ ExecutionContext, Future, Promise }
@InternalApi
private[projection] object SlickProjectionImpl {
  sealed trait Strategy
  case object ExactlyOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Int, orAfterDuration: FiniteDuration) extends Strategy
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, Envelope, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: Option[Offset] => Source[Envelope, _],
    offsetExtractor: Envelope => Offset,
    databaseConfig: DatabaseConfig[P],
    strategy: SlickProjectionImpl.Strategy,
    eventHandler: Envelope => DBIO[Done])
    extends Projection[Envelope] {
  import SlickProjectionImpl._

  private val offsetStore = new SlickOffsetStore(databaseConfig.db, databaseConfig.profile)

  private val killSwitch = KillSwitches.shared(projectionId.id)
  private val promiseToStop: Promise[Done] = Promise()

  private val started = new AtomicBoolean(false)

  override def run()(implicit systemProvider: ClassicActorSystemProvider): Unit = {
    if (started.compareAndSet(false, true)) {
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

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with `processEnvelope`, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(
      implicit systemProvider: ClassicActorSystemProvider): Source[Done, _] = {
    implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

    // TODO: add a LogSource for projection when we have a name and key
    val akkaLogger = Logging(systemProvider.classicSystem, this.getClass)

    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val futSource = lastKnownOffset.map { offsetOpt =>
      akkaLogger.debug("Starting projection [{}] from offset [{}]", projectionId, offsetOpt)
      sourceProvider(offsetOpt)
    }

    val handlerFlow: Flow[Envelope, Done, _] =
      strategy match {
        case ExactlyOnce =>
          Flow[Envelope]
            .mapAsync(1)(processEnvelopeAndStoreOffsetInSameTransaction)

        case AtLeastOnce(1, _) =>
          Flow[Envelope]
            .mapAsync(1)(processEnvelopeAndStoreOffsetInSeparateTransactions)

        case AtLeastOnce(afterEnvelopes, orAfterDuration) =>
          Flow[Envelope]
            .mapAsync(1) { env => processEnvelope(env).map(_ => offsetExtractor(env)) }
            .groupedWithin(afterEnvelopes, orAfterDuration)
            .collect { case grouped if grouped.nonEmpty => grouped.last }
            .mapAsync(parallelism = 1)(storeOffset)
      }

    Source
      .futureSource(futSource)
      .via(killSwitch.flow)
      .via(handlerFlow)
  }

  private def processEnvelopeAndStoreOffsetInSameTransaction(env: Envelope)(
      implicit ec: ExecutionContext): Future[Done] = {
    import databaseConfig.profile.api._

    // run user function and offset storage on the same transaction
    // any side-effect in user function is at-least-once
    val txDBIO =
      offsetStore
        .saveOffset(projectionId, offsetExtractor(env))
        .flatMap(_ => eventHandler(env))

    databaseConfig.db.run(txDBIO.transactionally).map(_ => Done)
  }

  private def processEnvelopeAndStoreOffsetInSeparateTransactions(env: Envelope)(
      implicit ec: ExecutionContext): Future[Done] = {

    val dbio = eventHandler(env).flatMap(_ => offsetStore.saveOffset(projectionId, offsetExtractor(env)))

    databaseConfig.db.run(dbio).map(_ => Done)
  }

  private def processEnvelope(env: Envelope)(implicit ec: ExecutionContext): Future[Done] = {
    import databaseConfig.profile.api._

    val dbio = eventHandler(env)
    databaseConfig.db.run(dbio.transactionally).map(_ => Done)
  }

  private def storeOffset(offset: Offset)(implicit ec: ExecutionContext): Future[Done] = {
    import databaseConfig.profile.api._

    val dbio = offsetStore.saveOffset(projectionId, offset)
    databaseConfig.db.run(dbio.transactionally).map(_ => Done)
  }
}
