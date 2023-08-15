/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata

import scala.concurrent.Future

/**
 * Interceptor allowing for example authentication/authorization of incoming connections to a [[EventProducerPushDestination]]
 */
@ApiMayChange
@FunctionalInterface
trait EventDestinationInterceptor {

  /**
   * Let's requests through if method returns, can fail request by throwing a [[akka.grpc.GrpcServiceException]]
   */
  def intercept(streamId: String, requestMetadata: Metadata): Future[Done]

}
