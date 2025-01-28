/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.BiFunction
import java.util.function.{ Function => JFunction }
import java.util.{ List => JList }

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._
import scala.jdk.FutureConverters._
import scala.jdk.OptionConverters._

import akka.actor.typed.ActorSystem
import akka.japi.function.Procedure3
import akka.projection.dynamodb.scaladsl
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse

object Requests {

  /**
   * Perform a DynamoDB batch write, retrying writes for any unprocessed items (with exponential backoff).
   *
   * @param client the DynamoDB async client to use
   * @param request the batch write request to apply
   * @param maxRetries max retries, when reached the resulting future will be failed with `failOnMaxRetries`
   * @param minBackoff minimum duration to backoff between retries
   * @param maxBackoff maximum duration to backoff between retries
   * @param randomFactor adds jitter to the retry delay (use 0 for no jitter)
   * @param system the actor system (for scheduling)
   * @return when successful return all responses from attempts,
   *         otherwise [[akka.projection.dynamodb.Requests.BatchWriteFailed]] with last response
   */
  def batchWriteWithRetries(
      client: DynamoDbAsyncClient,
      request: BatchWriteItemRequest,
      maxRetries: Int,
      minBackoff: Duration,
      maxBackoff: Duration,
      randomFactor: Double,
      system: ActorSystem[_]): CompletionStage[JList[BatchWriteItemResponse]] =
    scaladsl.Requests
      .batchWriteWithRetries(client, request, maxRetries, minBackoff.toScala, maxBackoff.toScala, randomFactor)(system)
      .map(_.asJava)(ExecutionContext.parasitic)
      .asJava

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
   * @param system the actor system (for scheduling)
   * @return all responses from attempts (in order)
   */
  def retryWithBackoff[Request, Response](
      request: Request,
      attempt: JFunction[Request, CompletionStage[Response]],
      decideRetry: BiFunction[Request, Response, Optional[Request]],
      maxRetries: Int,
      minBackoff: Duration,
      maxBackoff: Duration,
      randomFactor: Double,
      onRetry: Procedure3[Response, Integer, Duration],
      failOnMaxRetries: JFunction[Response, Throwable],
      system: ActorSystem[_]): CompletionStage[JList[Response]] =
    scaladsl.Requests
      .retryWithBackoff(
        request,
        (request: Request) => attempt.apply(request).asScala,
        (request: Request, response: Response) => decideRetry.apply(request, response).toScala,
        maxRetries,
        minBackoff.toScala,
        maxBackoff.toScala,
        randomFactor,
        (response: Response, retries: Int, delay: FiniteDuration) => onRetry.apply(response, retries, delay.toJava),
        (response: Response) => failOnMaxRetries.apply(response))(system)
      .map(_.asJava)(ExecutionContext.parasitic)
      .asJava
}
