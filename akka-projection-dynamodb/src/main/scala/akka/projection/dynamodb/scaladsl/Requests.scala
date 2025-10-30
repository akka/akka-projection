/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.dynamodb.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.jdk.FutureConverters._

import akka.actor.typed.ActorSystem
import akka.projection.dynamodb.Requests.BatchWriteFailed
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

object Requests {
  import akka.projection.dynamodb.Requests.batchUnprocessedTotal
  import akka.projection.dynamodb.Requests.log
  import akka.projection.dynamodb.Requests.retry

  /**
   * Perform a DynamoDB batch write, retrying writes for any unprocessed items (with exponential backoff).
   *
   * @param client the DynamoDB async client to use
   * @param request the batch write request to apply
   * @param maxRetries max retries, when reached the resulting future will be failed with `failOnMaxRetries`
   * @param minBackoff minimum duration to backoff between retries
   * @param maxBackoff maximum duration to backoff between retries
   * @param randomFactor adds jitter to the retry delay (use 0 for no jitter)
   * @return when successful return all responses from attempts,
   *         otherwise [[akka.projection.dynamodb.Requests.BatchWriteFailed]] with last response
   */
  def batchWriteWithRetries(
      client: DynamoDbAsyncClient,
      request: BatchWriteItemRequest,
      maxRetries: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double)(implicit system: ActorSystem[_]): Future[Seq[BatchWriteItemResponse]] =
    retryWithBackoff(
      request,
      (request: BatchWriteItemRequest) => client.batchWriteItem(request).asScala,
      (request: BatchWriteItemRequest, response: BatchWriteItemResponse) =>
        if (response.hasUnprocessedItems && !response.unprocessedItems.isEmpty)
          Some(request.toBuilder.requestItems(response.unprocessedItems).build())
        else None,
      maxRetries,
      minBackoff,
      maxBackoff,
      randomFactor,
      onRetry = (response: BatchWriteItemResponse, retry: Int, delay: FiniteDuration) =>
        if (log.isDebugEnabled) {
          log.debug(
            "Not all items in batch were written, [{}] unprocessed items, retrying in [{} ms], [{}/{}] retries",
            batchUnprocessedTotal(response),
            delay.toMillis,
            retry,
            maxRetries)
        },
      failOnMaxRetries = new BatchWriteFailed(_))

  /**
   * Retry generic requests with exponential backoff.
   *
   * The retry condition is controlled by the `decideRetry` function. It takes the last request and its response,
   * and if the request should be retried then it can return the next request to attempt.
   *
   * @param request the initial request
   * @param attempt attempt a request, returning a future of the response
   * @param decideRetry retry condition decision function, based on the request and response, returning next request
   * @param maxRetries max retries, when reached the resulting future will be failed with `failOnMaxRetries`
   * @param minBackoff minimum duration to backoff between retries
   * @param maxBackoff maximum duration to backoff between retries
   * @param randomFactor adds jitter to the retry delay (use 0 for no jitter)
   * @param onRetry called before each retry, with the response, the current retry count, and the delay for this retry
   * @param failOnMaxRetries if max retries is reached, create a throwable for the failed future result
   * @return all responses from attempts (in order)
   */
  def retryWithBackoff[Request, Response](
      request: Request,
      attempt: Request => Future[Response],
      decideRetry: (Request, Response) => Option[Request],
      maxRetries: Int,
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      onRetry: (Response, Int, FiniteDuration) => Unit,
      failOnMaxRetries: Response => Throwable)(implicit system: ActorSystem[_]): Future[Seq[Response]] =
    retry(request, attempt, decideRetry, maxRetries, minBackoff, maxBackoff, randomFactor, onRetry, failOnMaxRetries)
}
