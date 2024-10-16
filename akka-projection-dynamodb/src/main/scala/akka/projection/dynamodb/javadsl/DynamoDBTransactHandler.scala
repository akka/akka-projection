/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }
import java.util.{ Collection => JCollection }

import akka.Done
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.projection.javadsl.HandlerLifecycle
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
   * One envelope is processed at a time. It will not be invoked with the next envelope until the returned
   * CompletionStage has completed and the given items written in a transaction.
   */
  def process(envelope: Envelope): CompletionStage[JCollection[TransactWriteItem]]

  def start(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

  def stop(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)
}

@ApiMayChange
object DynamoDBTransactHandler {

  /**
   * INTERNAL API
   */
  @InternalApi
  private class DynamoDBTransactHandlerFunction[Envelope](
      handler: JFunction[Envelope, CompletionStage[JCollection[TransactWriteItem]]])
      extends DynamoDBTransactHandler[Envelope] {

    override def process(envelope: Envelope): CompletionStage[JCollection[TransactWriteItem]] = handler.apply(envelope)
  }

  /** DynamoDBTransactHandler that can be defined with a simple function */
  def fromFunction[Envelope](handler: JFunction[Envelope, CompletionStage[JCollection[TransactWriteItem]]])
      : DynamoDBTransactHandler[Envelope] =
    new DynamoDBTransactHandlerFunction(handler)
}
