/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.StatusObserver
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.InternalProjection
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickProjectionImpl
import akka.projection.slick.internal.SlickSettings
import akka.stream.scaladsl.FlowWithContext
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

/**
 * Factories of [[Projection]] where the offset is stored in a relational database table using Slick.
 * The envelope handler can integrate with anything, such as publishing to a message broker, or updating a relational read model.
 */
@ApiMayChange
object SlickProjection {

  /**
   * Create a [[Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   *
   */
  def exactlyOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[Envelope])(
      implicit system: ActorSystem[_]): ExactlyOnceSlickProjection[Offset, Envelope] = {

    // lift to Future handler
    val liftedHandler =
      new Handler[Envelope] {

        val offsetStore: SlickOffsetStore[P] =
          new SlickOffsetStore(databaseConfig.db, databaseConfig.profile, SlickSettings(system))

        implicit val ec = system.executionContext

        import databaseConfig.profile.api._
        override def process(envelope: Envelope): Future[Done] = {

          val offset = sourceProvider.extractOffset(envelope)
          val handlerAction = handler.process(envelope)

          val logger = Logging(system.classicSystem, classOf[SlickProjectionImpl[_, _, _]])

          sourceProvider.verifyOffset(offset) match {
            case VerificationSuccess =>
              // run user function and offset storage on the same transaction
              // any side-effect in user function is at-least-once
              val txDBIO = offsetStore
                .saveOffset(projectionId, offset)
                .flatMap(_ => handlerAction)
                .transactionally
              databaseConfig.db.run(txDBIO).map(_ => Done)
            case VerificationFailure(reason) =>
              logger.warning(
                "The offset failed source provider verification after the envelope was processed. " +
                "The transaction will not be executed. Skipping envelope with reason: {}",
                reason)
              Future.successful(Done)
          }

        }
        override def start(): Future[Done] = handler.start()
        override def stop(): Future[Done] = handler.stop()
      }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      SingleHandlerStrategy(liftedHandler),
      NoopStatusObserver)
  }

  /**
   * Create a [[Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceSlickProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    import databaseConfig.profile.api._

    // lift to Future handler
    val liftedHandler = new Handler[Envelope] {
      implicit val ec = system.executionContext
      override def process(envelope: Envelope): Future[Done] = {
        // user function in one transaction (may be composed of several DBIOAction)
        val dbio = handler.process(envelope).map(_ => Done).transactionally
        databaseConfig.db.run(dbio)
      }
      override def start(): Future[Done] = handler.start()
      override def stop(): Future[Done] = handler.stop()
    }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(liftedHandler),
      NoopStatusObserver)
  }

  /**
   * Create a [[Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedSlickProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   */
  def groupedWithin[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[immutable.Seq[Envelope]])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    // lift to Future handler
    val liftedHandler = new Handler[immutable.Seq[Envelope]] {

      val offsetStore: SlickOffsetStore[P] =
        new SlickOffsetStore(databaseConfig.db, databaseConfig.profile, SlickSettings(system))
      implicit val ec = system.executionContext
      import databaseConfig.profile.api._

      val logger = Logging(system.classicSystem, classOf[SlickProjectionImpl[_, _, _]])

      override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {

        val lastOffset = sourceProvider.extractOffset(envelopes.last)
        val handlerAction = handler.process(envelopes)

        sourceProvider.verifyOffset(lastOffset) match {
          case VerificationSuccess =>
            // run user function and offset storage on the same transaction
            // any side-effect in user function is at-least-once
            val txDBIO =
              offsetStore.saveOffset(projectionId, lastOffset).flatMap(_ => handlerAction).transactionally
            databaseConfig.db.run(txDBIO).mapTo[Done]
          case VerificationFailure(reason) =>
            logger.warning(
              "The offset failed source provider verification after the envelope was processed. " +
              "The transaction will not be executed. Skipping envelope(s) with reason: {}",
              reason)
            Future.successful(Done)
        }
      }
      override def start(): Future[Done] = handler.start()
      override def stop(): Future[Done] = handler.stop()
    }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      GroupedHandlerStrategy(liftedHandler),
      NoopStatusObserver)
  }

  /**
   * Create a [[Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once processing
   * semantics.
   *
   * The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried
   * in the context of the `FlowWithContext` and is stored in Cassandra when corresponding `Done` is emitted.
   * Since the offset is stored after processing the envelope it means that if the
   * projection is restarted from previously stored offset then some envelopes may be processed more than once.
   *
   * If the flow filters out envelopes the corresponding offset will not be stored, and such envelope
   * will be processed again if the projection is restarted and no later offset was stored.
   *
   * The flow should not duplicate emitted envelopes (`mapConcat`) with same offset, because then it can result in
   * that the first offset is stored and when the projection is restarted that offset is considered completed even
   * though more of the duplicated enveloped were never processed.
   *
   * The flow must not reorder elements, because the offsets may be stored in the wrong order and
   * and when the projection is restarted all envelopes up to the latest stored offset are considered
   * completed even though some of them may not have been processed. This is the reason the flow is
   * restricted to `FlowWithContext` rather than ordinary `Flow`.
   */
  def atLeastOnceFlow[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: FlowWithContext[Envelope, Envelope, Done, Envelope, _]): AtLeastOnceFlowProjection[Offset, Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler),
      NoopStatusObserver)

}

trait SlickProjection[Offset, Envelope] extends InternalProjection[Offset, Envelope] {

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): SlickProjection[Offset, Envelope]

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): SlickProjection[Offset, Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): SlickProjection[Offset, Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done]
}

trait ExactlyOnceSlickProjection[Offset, Envelope] extends SlickProjection[Offset, Envelope] {
  private[slick] def exactlyOnceStrategy: ExactlyOnce = offsetStrategy.asInstanceOf[ExactlyOnce]

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): ExactlyOnceSlickProjection[Offset, Envelope]

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): ExactlyOnceSlickProjection[Offset, Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): ExactlyOnceSlickProjection[Offset, Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): ExactlyOnceSlickProjection[Offset, Envelope]
}

object SlickHandler {

  /** SlickHandler that can be define from a simple function */
  private class SlickHandlerFunction[Envelope](handler: Envelope => DBIO[Done]) extends SlickHandler[Envelope] {
    override def process(envelope: Envelope): DBIO[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => DBIO[Done]): SlickHandler[Envelope] = new SlickHandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in [[SlickProjection]].
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Supported error handling strategies for when processing an `Envelope` fails can be
 * defined in configuration or using the `withRecoveryStrategy` method of a `Projection`
 * implementation.
 */
@ApiMayChange
trait SlickHandler[Envelope] extends HandlerLifecycle {

  def process(envelope: Envelope): DBIO[Done]

}
