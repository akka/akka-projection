/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
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
@InternalApi private[projection] object HandlerRecoveryImpl {

  private val futDone = Future.successful(Done)

  def apply[Offset, Envelope](
      projectionId: ProjectionId,
      recoveryStrategy: HandlerRecoveryStrategy,
      logger: LoggingAdapter,
      statusObserver: StatusObserver[Envelope],
      telemetry: Telemetry): HandlerRecoveryImpl[Offset, Envelope] =
    new HandlerRecoveryImpl(projectionId, recoveryStrategy, logger, statusObserver, telemetry)

}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class HandlerRecoveryImpl[Offset, Envelope](
    projectionId: ProjectionId,
    recoveryStrategy: HandlerRecoveryStrategy,
    logger: LoggingAdapter,
    statusObserver: StatusObserver[Envelope],
    telemetry: Telemetry) {
  def applyRecovery(
      env: Envelope,
      firstOffset: Offset, // used for logging
      lastOffset: Offset, // used for logging
      abort: Future[Done], // retries can be aborted by failing this Future
      futureCallback: () => Future[Done])(implicit system: ActorSystem[_]): Future[Done] = {
    import HandlerRecoveryImpl.futDone
    import HandlerRecoveryStrategy.Internal._

    implicit val scheduler: Scheduler = system.classicSystem.scheduler
    implicit val executionContext: ExecutionContext = system.executionContext

    val tryFutureCallback: () => Future[Done] = { () =>
      if (abort.isCompleted) {
        abort
      } else {
        try {
          futureCallback()
        } catch {
          case NonFatal(e) =>
            // in case the callback throws instead of returning failed Future
            Future.failed(e)
        }
      }
    }

    val retryFutureCallback: () => Future[Done] = { () =>
      tryFutureCallback().recoverWith {
        // using recoverWith instead of `.failed.foreach` to make sure that calls to statusObserver
        // are invoked in sequential order
        case err if !abort.isCompleted =>
          telemetry.error(err)
          statusObserver.error(projectionId, env, err, recoveryStrategy)
          Future.failed(err)
      }
    }

    // this will count as one attempt
    val firstAttempt = tryFutureCallback()

    def offsetLogParameter: String =
      if (firstOffset == lastOffset) s"envelope with offset [$firstOffset]"
      else s"envelopes with offsets from [$firstOffset] to [$lastOffset]"

    firstAttempt.recoverWith {
      case _ if abort.isCompleted =>
        abort
      case err =>
        telemetry.error(err)
        statusObserver.error(projectionId, env, err, recoveryStrategy)
        def delayFunction(delay: FiniteDuration): Int => Option[FiniteDuration] = { _ =>
          abort.value match {
            case None             => Some(delay)
            case Some(Success(_)) =>
              // abort immediately, will return the completed `abort: Future` from tryFutureCallback
              Some(Duration.Zero)
            case Some(Failure(abortErr)) =>
              // by throwing from the delay function the `retry` will stop
              throw abortErr
          }
        }

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
              "[{}] Failed to process {}. " +
              "Envelope will be skipped as defined by recovery strategy. Exception: {}",
              projectionId.id,
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
            val retried = after(delay, scheduler)(retry(retryFutureCallback, retries - 1, delayFunction(delay)))
            retried.failed.foreach { exception =>
              if (!abort.isCompleted)
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
            val retried = after(delay, scheduler)(retry(retryFutureCallback, retries - 1, delayFunction(delay)))
            retried.failed.foreach { exception =>
              logger.warning(
                "[{}] Failed to process {} after [{}] attempts. " +
                "Envelope will be skipped as defined by recovery strategy. Last exception: {}",
                projectionId.id,
                offsetLogParameter,
                retries + 1,
                exception)
            }
            retried.recoverWith { case _ => futDone }
        }
    }
  }

}
