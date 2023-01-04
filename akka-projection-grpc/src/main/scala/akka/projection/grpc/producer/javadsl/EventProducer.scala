/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.dispatch.ExecutionContexts
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.japi.function.{ Function => JapiFunction }
import akka.projection.grpc.internal.EventProducerServiceImpl
import akka.projection.grpc.internal.proto.EventProducerServicePowerApiHandler
import akka.util.ccompat.JavaConverters._

import java.util.Collections
import java.util.Optional
import java.util.concurrent.CompletionStage
import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters.RichOptionalGeneric

/**
 * The event producer implementation that can be included a gRPC route in an Akka HTTP server.
 */
@ApiMayChange
object EventProducer {

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   *
   * @param source The source that should be available from this event producer
   */
  def grpcServiceHandler(
      system: ActorSystem[_],
      source: EventProducerSource): JapiFunction[HttpRequest, CompletionStage[HttpResponse]] =
    grpcServiceHandler(system, Collections.singleton(source))

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   *
   * @param sources All sources that should be available from this event producer
   */
  def grpcServiceHandler(
      system: ActorSystem[_],
      sources: java.util.Set[EventProducerSource]): JapiFunction[HttpRequest, CompletionStage[HttpResponse]] =
    grpcServiceHandler(system, sources, Optional.empty())

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   *
   * @param sources All sources that should be available from this event producer
   * @param interceptor An optional request interceptor applied to each request to the service
   */
  def grpcServiceHandler(
      system: ActorSystem[_],
      sources: java.util.Set[EventProducerSource],
      interceptor: Optional[EventProducerInterceptor]): JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = {
    val scalaProducerSources = sources.asScala.map(_.asScala).toSet
    val eventsBySlicesQueriesPerStreamId =
      akka.projection.grpc.producer.scaladsl.EventProducer
        .eventsBySlicesQueriesForStreamIds(scalaProducerSources, system)

    val eventProducerService = new EventProducerServiceImpl(
      system,
      eventsBySlicesQueriesPerStreamId,
      scalaProducerSources,
      interceptor.asScala.map(new EventProducerInterceptorAdapter(_)),
      includeMetadata = false)

    val handler = EventProducerServicePowerApiHandler(eventProducerService)(system)
    new JapiFunction[HttpRequest, CompletionStage[HttpResponse]] {
      override def apply(request: HttpRequest): CompletionStage[HttpResponse] =
        handler(request.asInstanceOf[akka.http.scaladsl.model.HttpRequest])
          .map(_.asInstanceOf[HttpResponse])(ExecutionContexts.parasitic)
          .toJava
    }
  }

}
