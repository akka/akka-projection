/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import scala.concurrent.Future
import scala.reflect.ClassTag

import akka.actor.typed.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.projection.grpc.internal.EventProducerServiceImpl
import akka.projection.grpc.internal.proto.EventProducerServiceHandler
import akka.projection.grpc.producer.EventProducerSettings

/**
 * The event producer implementation that can be included a gRPC route in an Akka HTTP server.
 */
object EventProducer {

  object Transformation {
    val empty: Transformation = new Transformation(
      mappers = Map.empty,
      orElse = event =>
        Future.failed(
          new IllegalArgumentException(
            s"Missing transformation for event [${event.getClass}]")))

    /**
     * No transformation. Pass through each event as is.
     */
    val identity: Transformation = new Transformation(
      mappers = Map.empty,
      orElse = event => Future.successful(Option(event)))
  }

  /**
   * Transformation of events to the external (public) representation.
   * Events can be excluded by mapping them to `None`.
   */
  final class Transformation private (
      val mappers: Map[Class[_], Any => Future[Option[Any]]],
      val orElse: Any => Future[Option[Any]]) {

    def registerAsyncMapper[A: ClassTag, B](
        f: A => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(
        mappers.updated(clazz, f.asInstanceOf[Any => Future[Option[Any]]]),
        orElse)
    }

    def registerMapper[A: ClassTag, B](f: A => Option[B]): Transformation = {
      registerAsyncMapper[A, B](event => Future.successful(f(event)))
    }

    def registerAsyncOrElseMapper(
        f: Any => Future[Option[Any]]): Transformation = {
      new Transformation(mappers, f)
    }

    def registerOrElseMapper(f: Any => Option[Any]): Transformation = {
      registerAsyncOrElseMapper(event => Future.successful(f(event)))
    }
  }

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   */
  def grpcServiceHandler(transformation: Transformation)(
      implicit system: ActorSystem[_])
      : PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] = {
    grpcServiceHandler(transformation, EventProducerSettings(system))
  }

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   */
  def grpcServiceHandler(
      transformation: Transformation,
      settings: EventProducerSettings)(implicit system: ActorSystem[_])
      : PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] = {
    require(
      settings.queryPluginId.nonEmpty,
      s"Configuration property [akka.projection.grpc.producer.query-plugin-id] must be defined.")
    val eventsBySlicesQuery =
      PersistenceQuery(system)
        .readJournalFor[EventsBySliceQuery](settings.queryPluginId)

    val eventProducerService =
      new EventProducerServiceImpl(
        system,
        eventsBySlicesQuery,
        transformation,
        settings)

    EventProducerServiceHandler.partial(eventProducerService)
  }
}
