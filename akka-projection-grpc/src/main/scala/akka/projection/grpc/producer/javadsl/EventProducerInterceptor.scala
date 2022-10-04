/*
 * Copyright (C) 2009-2022 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.projection.grpc.producer.javadsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.grpc.javadsl.Metadata

import java.util.concurrent.CompletionStage

/**
 * Interceptor allowing for example authentication/authorization of incoming requests to consume a specific stream.
 */
@ApiMayChange
@FunctionalInterface
trait EventProducerInterceptor {

  /**
   * Let's requests through if method returns, can fail request by throwing a [[akka.grpc.GrpcServiceException]]
   */
  def intercept(streamId: String, requestMetadata: Metadata): CompletionStage[Done]

}
