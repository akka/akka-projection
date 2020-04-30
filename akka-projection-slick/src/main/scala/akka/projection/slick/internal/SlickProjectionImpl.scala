/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick.internal

import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.control.NonFatal

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.event.Logging
import akka.pattern._
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.Fail
import akka.projection.slick.RetryAndFail
import akka.projection.slick.RetryAndSkip
import akka.projection.slick.Skip
import akka.projection.slick.SlickHandler
import akka.stream.KillSwitches
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

@InternalApi
private[projection] object SlickProjectionImpl {
  sealed trait Strategy
  case object ExactlyOnce extends Strategy
  final case class AtLeastOnce(afterEnvelopes: Int, orAfterDuration: FiniteDuration) extends Strategy
}

@InternalApi
private[projection] class SlickProjectionImpl[Offset, Envelope, P <: JdbcProfile](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    databaseConfig: DatabaseConfig[P],
    strategy: SlickProjectionImpl.Strategy,
    eventHandler: SlickHandler[Envelope])
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

    import databaseConfig.profile.api._
    implicit val scheduler = systemProvider.classicSystem.scheduler
    implicit val dispatcher = systemProvider.classicSystem.dispatcher

    // TODO: add a LogSource for projection when we have a name and key
    val logger = Logging(systemProvider.classicSystem, this.getClass)

    val futDone = Future.successful(Done)

    def applyUserRecovery(envelope: Envelope)(futureCallback: () => Future[Done]) = {

      // this will count as one attempt
      val firstAttempt = futureCallback()

      firstAttempt.recoverWith {
        case NonFatal(err) =>
          val failedOffset = sourceProvider.extractOffset(envelope)

          eventHandler.onFailure(envelope, err) match {
            case Fail =>
              logger.error(
                cause = err,
                template =
                  "Failed to process envelope with offset [{}]. Projection will stop as defined by recovery strategy.",
                failedOffset)
              firstAttempt

            case Skip =>
              logger.warning(
                template =
                  "Failed to process envelope with offset [{}]. Envelope will be skipped as defined by recovery strategy. Exception {}({})",
                failedOffset,
                err.getClass.getSimpleName,
                err.getMessage)
              futDone

            case RetryAndFail(retries, delay) =>
              val totalRetries = retries - 1
              logger.error(
                cause = err,
                template = "Failed to process envelope with offset [{}] failed. Will retry {} time(s).",
                failedOffset,
                totalRetries)

              // less one attempt
              val retied = retry(futureCallback, totalRetries, delay)
              retied.onComplete {
                case Failure(exception) =>
                  logger.error(
                    cause = exception,
                    template =
                      "Failed to process envelope with offset [{}] after {} retries. Projection will stop as defined by recovery strategy.",
                    failedOffset,
                    totalRetries)
                case _ => // do nothing
              }
              retied

            case RetryAndSkip(retries, delay) =>
              // less one attempt
              val totalRetries = retries - 1
              logger.warning(
                template =
                  "Failed to process envelope with offset [{}] failed. Will retry {} time(s). Exception {}({})",
                failedOffset,
                totalRetries,
                err.getClass.getSimpleName,
                err.getMessage)

              retry(futureCallback, totalRetries, delay).recoverWith {
                case NonFatal(exception) =>
                  logger.warning(
                    template =
                      "Failed to process envelope with offset [{}] after {} retries. Envelope will be skipped as defined by recovery strategy. Last exception {}({})",
                    failedOffset,
                    totalRetries,
                    exception.getClass.getSimpleName,
                    exception.getMessage)
                  futDone
              }
          }
      }
    }

    def aroundUserHandler(env: Envelope)(handlerFunc: Envelope => slick.dbio.DBIO[Done]) = {
      try {
        handlerFunc(env)
      } catch {
        case NonFatal(err) =>
          eventHandler.onFailure(env, err) match {

            case Fail =>
              logger.warning(
                template =
                  "An exception was thrown from inside the handler function while processing envelope with offset [{}]. Projection will stop as defined by recovery strategy.",
                sourceProvider.extractOffset(env),
                err.getClass.getSimpleName,
                err.getMessage)
              throw err

            case Skip =>
              logger.warning(
                template =
                  "An exception was thrown from inside the handler function while processing envelope with offset [{}]. Envelope will be skipped as defined by recovery strategy. Exception {}({})",
                sourceProvider.extractOffset(env),
                err.getClass.getSimpleName,
                err.getMessage)
              slick.dbio.DBIO.successful(Done)

            case _: RetryAndSkip =>
              logger.warning(
                template =
                  "An exception was thrown from inside the handler function while processing envelope with offset [{}]. This won't be retried. Envelope will be skipped as defined by recovery strategy. Exception {}({})",
                sourceProvider.extractOffset(env),
                err.getClass.getSimpleName,
                err.getMessage)
              slick.dbio.DBIO.successful(Done)

            case _: RetryAndFail =>
              logger.warning(
                template =
                  "An exception was thrown from inside the handler function while processing envelope with offset [{}]. This won't be retried. Projection will stop as defined by recovery strategy.",
                sourceProvider.extractOffset(env),
                err.getClass.getSimpleName,
                err.getMessage)
              throw err
          }
      }
    }

    def processEnvelopeAndStoreOffsetInSameTransaction(env: Envelope): Future[Done] = {
      // run user function and offset storage on the same transaction
      // any side-effect in user function is at-least-once
      val txDBIO =
        offsetStore
          .saveOffset(projectionId, sourceProvider.extractOffset(env))
          .flatMap(_ => aroundUserHandler(env)(eventHandler.handle))
          .transactionally

      applyUserRecovery(env) { () =>
        databaseConfig.db.run(txDBIO).map(_ => Done)
      }
    }

    def processEnvelopeAndStoreOffsetInSeparateTransactions(env: Envelope): Future[Done] = {
      // user function in one transaction (may be composed of several DBIOAction), and offset save in separate
      val dbio =
        aroundUserHandler(env)(eventHandler.handle).transactionally
          .flatMap(_ => offsetStore.saveOffset(projectionId, sourceProvider.extractOffset(env)))

      applyUserRecovery(env) { () =>
        databaseConfig.db.run(dbio).map(_ => Done)
      }
    }

    def processEnvelope(env: Envelope): Future[Done] = {
      // user function in one transaction (may be composed of several DBIOAction)
      val dbio = aroundUserHandler(env)(eventHandler.handle).transactionally
      applyUserRecovery(env) { () =>
        databaseConfig.db.run(dbio).map(_ => Done)
      }
    }

    def storeOffset(offset: Offset): Future[Done] = {
      // only one DBIOAction, no need for transactionally
      val dbio = offsetStore.saveOffset(projectionId, offset)
      databaseConfig.db.run(dbio).map(_ => Done)
    }

    // -------------------------------------------------------
    // finally build the source with all parts wired
    val lastKnownOffset: Future[Option[Offset]] = offsetStore.readOffset(projectionId)

    val futSource = lastKnownOffset.map { offsetOpt =>
      logger.debug("Starting projection [{}] from offset [{}]", projectionId, offsetOpt)
      sourceProvider.source(offsetOpt)
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
            .mapAsync(1) { env =>
              processEnvelope(env).map(_ => sourceProvider.extractOffset(env))
            }
            .groupedWithin(afterEnvelopes, orAfterDuration)
            .collect { case grouped if grouped.nonEmpty => grouped.last }
            .mapAsync(parallelism = 1)(storeOffset)
      }

    Source
      .futureSource(futSource)
      .via(killSwitch.flow)
      .via(handlerFlow)
  }

}
