/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.annotation.nowarn
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.event.Logging
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationFailureException
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.scaladsl.VerifiableSourceProvider
import akka.projection.slick.internal.SlickOffsetStore
import akka.projection.slick.internal.SlickProjectionImpl
import akka.projection.slick.internal.SlickSettings
import akka.stream.scaladsl.FlowWithContext
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

/**
 * Factories of [[akka.projection.Projection]] where the offset is stored in a relational database table using Slick.
 * The envelope handler can integrate with anything, such as publishing to a message broker, or updating a relational read model.
 */
@ApiMayChange
object SlickProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   */
  def exactlyOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: () => SlickHandler[Envelope])(
      implicit system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val offsetStore = createOffsetStore(databaseConfig)

    val adaptedSlickHandler: () => Handler[Envelope] = () =>
      new Handler[Envelope] {

        private implicit val ec: ExecutionContext = system.executionContext
        private val logger = Logging(system.classicSystem, classOf[SlickProjectionImpl[_, _, _]])
        private val delegate = handler()

        import databaseConfig.profile.api._
        override def process(envelope: Envelope): Future[Done] = {

          val offset = sourceProvider.extractOffset(envelope)
          val processedDBIO = offsetStore
            .saveOffset(projectionId, offset)
            .flatMap(_ => delegate.process(envelope))
          val verifiedDBIO =
            sourceProvider match {
              case vsp: VerifiableSourceProvider[Offset, Envelope] =>
                processedDBIO.flatMap { action =>
                  vsp.verifyOffset(offset) match {
                    case VerificationSuccess => slick.dbio.DBIO.successful(action)
                    case VerificationFailure(reason) =>
                      logger.warning(
                        "The offset failed source provider verification after the envelope was processed. " +
                        "The transaction will not be executed. Skipping envelope with reason: {}",
                        reason)
                      slick.dbio.DBIO.failed(VerificationFailureException)
                  }
                }
              case _ => processedDBIO
            }

          // run user function and offset storage on the same transaction
          // any side-effect in user function is at-least-once
          databaseConfig.db
            .run(verifiedDBIO.transactionally)
            .recover {
              case VerificationFailureException => Done
            }
            .map(_ => Done)
        }

        override def start(): Future[Done] = delegate.start()
        override def stop(): Future[Done] = delegate.stop()
      }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      SingleHandlerStrategy(adaptedSlickHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * The DBIO returned by the [[SlickHandler.process()]] of the provided [[handler]] will be
   * wrapped in a transaction.
   *
   * It stores the offset in a relational database table using Slick after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: () => SlickHandler[Envelope])(
      implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    import databaseConfig.profile.api._

    val adaptedSlickHandler: () => Handler[Envelope] = () =>
      new Handler[Envelope] {
        private implicit val ec: ExecutionContext = system.executionContext
        private val delegate = handler()

        override def process(envelope: Envelope): Future[Done] = {
          // user function in one transaction (may be composed of several DBIOAction)
          val dbio = delegate.process(envelope).map(_ => Done).transactionally
          databaseConfig.db.run(dbio)
        }
        override def start(): Future[Done] = delegate.start()
        override def stop(): Future[Done] = delegate.stop()
      }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(adaptedSlickHandler),
      NoopStatusObserver,
      createOffsetStore(databaseConfig))
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * Compared to [[SlickProjection.atLeastOnce]] the [[Handler]] is not storing the projected result in the
   * database, but is integrating with something else.
   *
   * It stores the offset in a relational database table using Slick after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnceAsync[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: () => Handler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(handler),
      NoopStatusObserver,
      createOffsetStore(databaseConfig))
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   */
  def groupedWithin[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: () => SlickHandler[immutable.Seq[Envelope]])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val offsetStore = createOffsetStore(databaseConfig)

    val adaptedSlickHandler: () => Handler[immutable.Seq[Envelope]] = () =>
      new Handler[immutable.Seq[Envelope]] {

        import databaseConfig.profile.api._
        private implicit val ec: ExecutionContext = system.executionContext
        private val logger = Logging(system.classicSystem, classOf[SlickProjectionImpl[_, _, _]])
        private val delegate = handler()

        override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {

          val lastOffset = sourceProvider.extractOffset(envelopes.last)
          val processedDBIO = offsetStore
            .saveOffset(projectionId, lastOffset)
            .flatMap(_ => delegate.process(envelopes))
          val verifiedDBIO =
            sourceProvider match {
              case vsp: VerifiableSourceProvider[Offset, Envelope] =>
                processedDBIO.flatMap { action =>
                  vsp.verifyOffset(lastOffset) match {
                    case VerificationSuccess => slick.dbio.DBIO.successful(action)
                    case VerificationFailure(reason) =>
                      logger.warning(
                        "The offset failed source provider verification after the envelope was processed. " +
                        "The transaction will not be executed. Skipping envelope with reason: {}",
                        reason)
                      slick.dbio.DBIO.failed(VerificationFailureException)
                  }
                }
              case _ => processedDBIO
            }

          // run user function and offset storage on the same transaction
          // any side-effect in user function is at-least-once
          databaseConfig.db
            .run(verifiedDBIO.transactionally)
            .recover {
              case VerificationFailureException => Done
            }
            .map(_ => Done)
        }

        override def start(): Future[Done] = delegate.start()
        override def stop(): Future[Done] = delegate.stop()
      }

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      GroupedHandlerStrategy(adaptedSlickHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * Compared to [[SlickProjection.groupedWithin]] the [[Handler]] is not storing the projected result in the
   * database, but is integrating with something else.
   *
   * It stores the offset in  a relational database table using Slick immediately after the `handler` has
   * processed the envelopes, but that is still with at-least-once processing semantics. This means that
   * if the projection is restarted from previously stored offset the previous group of envelopes may be
   * processed more than once.
   */
  def groupedWithinAsync[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: () => Handler[immutable.Seq[Envelope]])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val offsetStore = createOffsetStore(databaseConfig)

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(Duration.Zero)),
      GroupedHandlerStrategy(handler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once processing
   * semantics.
   *
   * The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried
   * in the context of the `FlowWithContext` and is stored in the database when corresponding `Done` is emitted.
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
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _])(
      implicit system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler),
      NoopStatusObserver,
      createOffsetStore(databaseConfig))
  }

  /**
   * For testing purposes the projection offset and management tables can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createTablesIfNotExists[P <: JdbcProfile: ClassTag](databaseConfig: DatabaseConfig[P])(
      implicit system: ActorSystem[_]): Future[Done] = {
    createOffsetStore(databaseConfig).createIfNotExists()
  }

  @deprecated("Renamed to createTablesIfNotExists", "1.2.0")
  def createOffsetTableIfNotExists[P <: JdbcProfile: ClassTag](databaseConfig: DatabaseConfig[P])(
      implicit system: ActorSystem[_]): Future[Done] =
    createTablesIfNotExists(databaseConfig)

  /**
   * For testing purposes the projection offset and management tables can be dropped programmatically.
   */
  def dropTablesIfExists[P <: JdbcProfile: ClassTag](databaseConfig: DatabaseConfig[P])(
      implicit system: ActorSystem[_]): Future[Done] = {
    createOffsetStore(databaseConfig).dropIfExists()
  }

  @deprecated("Renamed to dropTablesIfExists", "1.2.0")
  def dropOffsetTableIfExists[P <: JdbcProfile: ClassTag](databaseConfig: DatabaseConfig[P])(
      implicit system: ActorSystem[_]): Future[Done] =
    dropTablesIfExists(databaseConfig)

  @nowarn("msg=never used")
  private def createOffsetStore[P <: JdbcProfile: ClassTag](databaseConfig: DatabaseConfig[P])(
      implicit system: ActorSystem[_]) =
    new SlickOffsetStore(system, databaseConfig.db, databaseConfig.profile, SlickSettings(system))
}

@ApiMayChange
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
