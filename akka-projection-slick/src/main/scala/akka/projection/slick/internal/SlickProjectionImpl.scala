/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.util.concurrent.atomic.AtomicBoolean

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.projection.slick.OffsetStore
import akka.projection.{ Projection, ProjectionId }
import akka.stream.KillSwitches
import akka.stream.scaladsl.{ Sink, Source }
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.{ ExecutionContext, Future, Promise }

@InternalApi
private[projection] class SlickProjectionImpl[Offset, StreamElement, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: Option[Offset] => Source[StreamElement, _],
    offsetExtractor: StreamElement => Offset,
    databaseConfig: DatabaseConfig[P],
    eventHandler: StreamElement => DBIO[Done])
    extends Projection[StreamElement] {

  private val offsetStore = new OffsetStore(databaseConfig.db, databaseConfig.profile)

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
   * The method wrapping the user EventHandler function.
   *
   * @return A [[scala.concurrent.Future]] that represents the asynchronous completion of the user EventHandler
   *         function.
   */
  override def processElement(elt: StreamElement)(implicit ec: ExecutionContext): Future[Done] = {
    import databaseConfig.profile.api._

    // run user function and offset storage on the same transaction
    // any side-effect in user function is at-least-once
    val txDBIO =
      offsetStore.saveOffset(projectionId, offsetExtractor(elt)).flatMap(_ => eventHandler(elt))

    databaseConfig.db.run(txDBIO.transactionally).map(_ => Done)
  }

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with `processElement`, but before any sink attached.
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

    Source.futureSource(futSource).via(killSwitch.flow).mapAsync(1)(processElement)
  }
}
