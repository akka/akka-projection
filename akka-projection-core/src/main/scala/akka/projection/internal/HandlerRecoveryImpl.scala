/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.Done
import akka.actor.Scheduler
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.LoggingAdapter
import akka.pattern.after
import akka.pattern.retry
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.StatusObserver

/**
 * INTERNAL API
 */
@InternalApi private[akka] object HandlerRecoveryImpl {

  private val futDone = Future.successful(Done)

  def apply[Offset, Envelope](
      projectionId: ProjectionId,
      recoveryStrategy: HandlerRecoveryStrategy,
      logger: LoggingAdapter,
      statusObserver: StatusObserver[Envelope]): HandlerRecoveryImpl[Offset, Envelope] =
    new HandlerRecoveryImpl(projectionId, recoveryStrategy, logger, statusObserver)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class HandlerRecoveryImpl[Offset, Envelope](
    projectionId: ProjectionId,
    recoveryStrategy: HandlerRecoveryStrategy,
    logger: LoggingAdapter,
    statusObserver: StatusObserver[Envelope]) {
  def applyRecovery(
      env: Envelope,
      firstOffset: Offset, // used for logging
      lastOffset: Offset, // used for logging
      futureCallback: () => Future[Done])(implicit system: ActorSystem[_]): Future[Done] = {
    import HandlerRecoveryImpl.futDone
    import HandlerRecoveryStrategy.Internal._

    implicit val scheduler: Scheduler = system.classicSystem.scheduler
    implicit val executionContext: ExecutionContext = system.executionContext

    val tryFutureCallback: () => Future[Done] = { () =>
      try {
        futureCallback()
      } catch {
        case NonFatal(e) =>
          // in case the callback throws instead of returning failed Future
          Future.failed(e)
      }
    }

    def retryFutureCallback(attemptCount: AtomicInteger): () => Future[Done] = { () =>
      tryFutureCallback().recoverWith {
        // using recoverWith instead of `.failed.foreach` to make sure that calls to statusObserver
        // are invoked in sequential order
        case err =>
          statusObserver.error(projectionId, env, err, attemptCount.incrementAndGet(), recoveryStrategy)
          Future.failed(err)
      }
    }

    // this will count as one attempt
    val firstAttempt = tryFutureCallback()

    def offsetLogParameter: String =
      if (firstOffset == lastOffset) s"envelope with offset [$firstOffset]"
      else s"envelopes with offsets from [$firstOffset] to [$lastOffset]"

    firstAttempt.recoverWith {
      case err =>
        statusObserver.error(projectionId, env, err, errorCount = 1, recoveryStrategy)

        recoveryStrategy match {
          case Fail =>
            logger.error(
              cause = err,
              template = "[{}] Failed to process {}. Projection will stop as defined by recovery strategy.",
              projectionId.id,
              offsetLogParameter)
            firstAttempt

          case Skip =>
            logger.warning(
              "Failed to process {}. " +
              "Envelope will be skipped as defined by recovery strategy. Exception: {}",
              offsetLogParameter,
              err)
            futDone

          case RetryAndFail(retries, delay) =>
            logger.warning(
              "[{}] First attempt to process {} failed. Will retry [{}] time(s). " +
              "Exception: {}",
              projectionId.id,
              offsetLogParameter,
              retries,
              err)

            // retries - 1 because retry() is based on attempts
            // first attempt is performed immediately and therefore we must first delay
            val attemptCount = new AtomicInteger(1)
            val retried = after(delay, scheduler)(retry(retryFutureCallback(attemptCount), retries - 1, delay))
            retried.failed.foreach { exception =>
              logger.error(
                cause = exception,
                template =
                  "[{}] Failed to process {} after [{}] attempts. " +
                  "Projection will stop as defined by recovery strategy.",
                projectionId,
                offsetLogParameter,
                retries + 1)
            }
            retried

          case RetryAndSkip(retries, delay) =>
            logger.warning(
              "[{}] First attempt to process {} failed. Will retry [{}] time(s). Exception: {}",
              projectionId.id,
              offsetLogParameter,
              retries,
              err)

            // retries - 1 because retry() is based on attempts
            // first attempt is performed immediately and therefore we must first delay
            val attemptCount = new AtomicInteger(1)
            val retried = after(delay, scheduler)(retry(retryFutureCallback(attemptCount), retries - 1, delay))
            retried.failed.foreach { exception =>
              logger.warning(
                "[{}] Failed to process {} after [{}] attempts. " +
                "Envelope will be skipped as defined by recovery strategy. Last exception: {}",
                projectionId.id,
                offsetLogParameter,
                retries + 1,
                exception)
            }
            retried.recoverWith(_ => futDone)
        }
    }
  }

}
