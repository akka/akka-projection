/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.producer.javadsl

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._

import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.projection.grpc.internal.EventProducerServiceImpl
import akka.projection.grpc.producer.EventProducerSettings
import akka.japi.function.{ Function => JapiFunction }
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.projection.grpc.internal.proto.EventProducerServiceHandler

/**
 * The event producer implementation that can be included a gRPC route in an Akka HTTP server.
 */
object EventProducer {

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   */
  def grpcServiceHandler(system: ActorSystem[_], transformation: Transformation)
      : JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = {
    grpcServiceHandler(system, transformation, EventProducerSettings(system))
  }

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   */
  def grpcServiceHandler(
      system: ActorSystem[_],
      transformation: Transformation,
      settings: EventProducerSettings)
      : JapiFunction[HttpRequest, CompletionStage[HttpResponse]] = {
    val eventsBySlicesQuery =
      PersistenceQuery
        .get(system)
        .readJournalFor[EventsBySliceQuery](settings.queryPluginId)

    val eventProducerService =
      new EventProducerServiceImpl(
        system,
        eventsBySlicesQuery,
        transformation.delegate)

    val handler = EventProducerServiceHandler(eventProducerService)(system)
    new JapiFunction[HttpRequest, CompletionStage[HttpResponse]] {
      override def apply(request: HttpRequest): CompletionStage[HttpResponse] =
        handler(request.asInstanceOf[akka.http.scaladsl.model.HttpRequest])
          .map(_.asInstanceOf[HttpResponse])(ExecutionContexts.parasitic)
          .toJava
    }
  }
}
