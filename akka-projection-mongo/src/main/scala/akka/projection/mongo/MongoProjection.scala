/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import scala.collection.immutable
import scala.concurrent.Future
import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.event.Logging
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.mongo.internal.{ MongoOffsetStore, MongoProjectionImpl, MongoSettings }
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext
import org.mongodb.scala.MongoClient

/**
 * Factories of [[akka.projection.Projection]] where the offset is stored in mongoDB.
 * The envelope handler can integrate with anything, such as publishing to a message broker, or updating a relational read model.
 */
@ApiMayChange
object MongoProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in mongoDB in the same transaction
   *
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      db: MongoClient,
      handler: MongoHandler[Envelope])(implicit system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val offsetStore: MongoOffsetStore =
      new MongoOffsetStore(db, MongoSettings(system))

    // lift to Future handler
    val liftedHandler =
      new Handler[Envelope] {

        implicit val ec = system.executionContext

        override def process(envelope: Envelope): Future[Done] = {

          val offset = sourceProvider.extractOffset(envelope)
          val handlerAction = handler.process(envelope)

          val logger = Logging(system.classicSystem, classOf[MongoProjectionImpl[_, _]])

          sourceProvider.verifyOffset(offset) match {
            case VerificationSuccess =>
              // run user function and offset storage on the same transaction
              // any side-effect in user function is at-least-once
              val txDBIO = offsetStore
                .saveOffset(projectionId, offset)
                .flatMap(_ => handlerAction)
              TransactionCtx.withSession(db.startSession())(txDBIO).map(_ => Done)
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

    new MongoProjectionImpl(
      projectionId,
      sourceProvider,
      db,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      SingleHandlerStrategy(liftedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a mongoDB collection after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      db: MongoClient,
      handler: MongoHandler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val offsetStore: MongoOffsetStore =
      new MongoOffsetStore(db, MongoSettings(system))

    val adaptedHandler = new Handler[Envelope] {
      implicit val ec = system.executionContext
      override def process(envelope: Envelope): Future[Done] = {
        // user function in one transaction (may be composed of several DBIOAction)
        val dbio = handler.process(envelope).map(_ => Done)
        TransactionCtx.withSession(db.startSession())(dbio)
      }
      override def start(): Future[Done] = handler.start()
      override def stop(): Future[Done] = handler.stop()
    }

    new MongoProjectionImpl(
      projectionId,
      sourceProvider,
      db,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(adaptedHandler),
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
   * It stores the offset in a mongoDB collection in the same transaction
   * as the DBIO returned from the `handler`.
   */
  def groupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      db: MongoClient,
      handler: MongoHandler[immutable.Seq[Envelope]])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val offsetStore: MongoOffsetStore =
      new MongoOffsetStore(db, MongoSettings(system))

    val adaptedHandler = new Handler[immutable.Seq[Envelope]] {

      implicit val ec = system.executionContext

      val logger = Logging(system.classicSystem, classOf[MongoProjectionImpl[_, _]])

      override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {

        val lastOffset = sourceProvider.extractOffset(envelopes.last)
        val handlerAction = handler.process(envelopes)

        sourceProvider.verifyOffset(lastOffset) match {
          case VerificationSuccess =>
            // run user function and offset storage on the same transaction
            // any side-effect in user function is at-least-once
            val txDBIO =
              offsetStore.saveOffset(projectionId, lastOffset).flatMap(_ => handlerAction)
            TransactionCtx.withSession(db.startSession())(txDBIO)
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

    new MongoProjectionImpl(
      projectionId,
      sourceProvider,
      db,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once processing
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
  def atLeastOnceFlow[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      db: MongoClient,
      handler: FlowWithContext[Envelope, Envelope, Done, Envelope, _])(
      implicit system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    val offsetStore: MongoOffsetStore =
      new MongoOffsetStore(db, MongoSettings(system))

    new MongoProjectionImpl(
      projectionId,
      sourceProvider,
      db,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler),
      NoopStatusObserver,
      offsetStore)
  }

  def createOffsetTableIfNotExists(db: MongoClient)(implicit system: ActorSystem[_]): Future[Done] = {
    val offsetStore: MongoOffsetStore =
      new MongoOffsetStore(db, MongoSettings(system))
    offsetStore.createIfNotExists
  }
}

object MongoHandler {

  /** MongoHandler that can be define from a simple function */
  private class MongoHandlerFunction[Envelope](handler: Envelope => TransactionCtx[Done])
      extends MongoHandler[Envelope] {
    override def process(envelope: Envelope): TransactionCtx[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => TransactionCtx[Done]): MongoHandler[Envelope] =
    new MongoHandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in [[MongoProjection]].
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
trait MongoHandler[Envelope] extends HandlerLifecycle {

  def process(envelope: Envelope): TransactionCtx[Done]

}
