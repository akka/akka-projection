/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.pattern.BackoffSupervisor
import akka.pattern.after
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

object Requests {

  final class BatchWriteFailed(val lastResponse: BatchWriteItemResponse)
      extends RuntimeException(
        s"Failed to batch write all items, [${batchUnprocessedTotal(lastResponse)}] unprocessed items remaining")

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] val log: Logger = LoggerFactory.getLogger(getClass)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def batchUnprocessedTotal(response: BatchWriteItemResponse): Int =
    response.unprocessedItems.asScala.valuesIterator.map(_.size).sum

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def retry[Request, Response](
      request: Request,
      attempt: Request => Future[Response],
      decideRetry: (Request, Response) => Option[Request],
      maxRetries: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      onRetry: (Response, Int, FiniteDuration) => Unit,
      failOnMaxRetries: Response => Throwable,
      retries: Int = 0)(implicit system: ActorSystem[_]): Future[Seq[Response]] = {
    import system.executionContext
    attempt(request).flatMap { response =>
      decideRetry(request, response) match {
        case Some(nextRequest) =>
          if (retries >= maxRetries) {
            Future.failed(failOnMaxRetries(response))
          } else { // retry after exponential backoff
            val nextRetry = retries + 1
            val delay = BackoffSupervisor.calculateDelay(retries, minBackoff, maxBackoff, randomFactor)
            onRetry(response, nextRetry, delay)
            after(delay) {
              retry(
                nextRequest,
                attempt,
                decideRetry,
                maxRetries,
                minBackoff,
                maxBackoff,
                randomFactor,
                onRetry,
                failOnMaxRetries,
                nextRetry)
            }.map { responses => response +: responses }(ExecutionContext.parasitic)
          }
        case None => Future.successful(Seq(response))
      }
    }
  }
}
