/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.javadsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.grpc.javadsl.Metadata

import java.util.concurrent.CompletionStage

/**
 * Interceptor allowing for example authentication/authorization of incoming connections to a [[EventProducerPushDestination]] */
@ApiMayChange
@FunctionalInterface
trait EventDestinationInterceptor {

  /**
   * Let's requests through if method returns, can fail request by throwing a [[akka.grpc.GrpcServiceException]]
   */
  def intercept(streamId: String, requestMetadata: Metadata): CompletionStage[Done]

}
