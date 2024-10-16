/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import akka.Done
import akka.grpc.javadsl.Metadata

import java.util.concurrent.CompletionStage

/**
 * Interceptor allowing for example authentication/authorization of incoming connections to a [[EventProducerPushDestination]] */
@FunctionalInterface
trait EventDestinationInterceptor {

  /**
   * Let's requests through if method returns, can fail request by throwing a [[akka.grpc.GrpcServiceException]]
   */
  def intercept(streamId: String, requestMetadata: Metadata): CompletionStage[Done]

}
