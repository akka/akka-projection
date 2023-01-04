/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.Done
import akka.annotation.ApiMayChange
import akka.grpc.internal.JavaMetadataImpl
import akka.grpc.javadsl.Metadata
import akka.grpc.scaladsl

import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters.CompletionStageOps
import scala.concurrent.Future

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

/**
 * INTERNAL API
 */
private[akka] final class EventProducerInterceptorAdapter(interceptor: EventProducerInterceptor)
    extends akka.projection.grpc.producer.scaladsl.EventProducerInterceptor {
  override def intercept(streamId: String, requestMetadata: scaladsl.Metadata): Future[Done] =
    interceptor
      .intercept(
        streamId,
        // FIXME: Akka gRPC internal class, add public API for Scala to Java metadata there
        new JavaMetadataImpl(requestMetadata))
      .toScala
}
