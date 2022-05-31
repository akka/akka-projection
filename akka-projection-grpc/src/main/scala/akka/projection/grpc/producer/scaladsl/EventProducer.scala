/**
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

object EventProducer {

  object Transformation {
    val empty: Transformation = new Transformation(
      mappers = Map.empty,
      orElse = event =>
        Future.failed(
          new IllegalArgumentException(
            s"Missing transformation for event [${event.getClass}]")))
  }

  /**
   * Transformation of events to the external (public) representation.
   * Events can be excluded by mapping them to `None`.
   */
  final class Transformation private (
      val mappers: Map[Class[_], Any => Future[Option[Any]]],
      val orElse: Any => Future[Option[Any]]) {

    def registerMapper[A: ClassTag, B](
        f: A => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(
        mappers.updated(clazz, f.asInstanceOf[Any => Future[Option[Any]]]),
        orElse)
    }

    def registerOrElseMapper(f: Any => Future[Option[Any]]): Unit = {
      new Transformation(mappers, f)
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
    val eventsBySlicesQuery =
      PersistenceQuery(system)
        .readJournalFor[EventsBySliceQuery](settings.queryPluginId)

    val eventProducerService =
      new EventProducerServiceImpl(system, eventsBySlicesQuery, transformation)

    EventProducerServiceHandler.partial(eventProducerService)
  }
}
