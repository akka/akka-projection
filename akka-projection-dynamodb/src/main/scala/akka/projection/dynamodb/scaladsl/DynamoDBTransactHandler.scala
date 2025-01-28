/*
 * Copyright (C) 2024-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.scaladsl

import scala.concurrent.Future

import akka.annotation.ApiMayChange
import akka.projection.scaladsl.HandlerLifecycle
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem

/**
 * Implement this interface for the Envelope handler for DynamoDB transactional projections.
 *
 * It can be stateful, with variables and mutable data structures. It is invoked by the `Projection` machinery one
 * envelope at a time and visibility guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Supported error handling strategies for when processing an `Envelope` fails can be defined in configuration or using
 * the `withRecoveryStrategy` method of a `Projection` implementation.
 */
@ApiMayChange
trait DynamoDBTransactHandler[Envelope] extends HandlerLifecycle {

  /**
   * The `process` method is invoked for each `Envelope`, and should return DynamoDB `TransactWriteItem`s to atomically
   * write along with the projection offset.
   *
   * One envelope is processed at a time. It will not be invoked with the next envelope until the returned Future has
   * completed and the given items written in a transaction.
   */
  def process(envelope: Envelope): Future[Iterable[TransactWriteItem]]
}

@ApiMayChange
object DynamoDBTransactHandler {

  private class DynamoDBTransactHandlerFunction[Envelope](handler: Envelope => Future[Iterable[TransactWriteItem]])
      extends DynamoDBTransactHandler[Envelope] {

    override def process(envelope: Envelope): Future[Iterable[TransactWriteItem]] = handler(envelope)
  }

  /** DynamoDBTransactHandler that can be defined with a simple function */
  def apply[Envelope](handler: Envelope => Future[Iterable[TransactWriteItem]]): DynamoDBTransactHandler[Envelope] =
    new DynamoDBTransactHandlerFunction(handler)
}
