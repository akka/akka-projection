/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import scala.concurrent.Future
import scala.reflect.ClassTag

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.projection.grpc.internal.EventProducerServiceImpl
import akka.projection.grpc.internal.proto.EventProducerServicePowerApiHandler
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.javadsl.{ Transformation => JTransformation }

/**
 * The event producer implementation that can be included a gRPC route in an Akka HTTP server.
 */
@ApiMayChange
object EventProducer {

  object EventProducerSource {
    def apply(
        entityType: String,
        streamId: String,
        transformation: Transformation,
        settings: EventProducerSettings): EventProducerSource =
      new EventProducerSource(entityType, streamId, transformation, settings, _ => true, transformSnapshot = None)

    def apply[Event](
        entityType: String,
        streamId: String,
        transformation: Transformation,
        settings: EventProducerSettings,
        producerFilter: EventEnvelope[Event] => Boolean): EventProducerSource =
      new EventProducerSource(
        entityType,
        streamId,
        transformation,
        settings,
        producerFilter.asInstanceOf[EventEnvelope[Any] => Boolean],
        transformSnapshot = None)

  }

  /**
   * @param entityType     The internal entity type name
   * @param streamId       The public, logical, stream id that consumers use to consume this source
   * @param transformation Transformations for turning the internal events to public message types
   * @param settings       The event producer settings used (can be shared for multiple sources)
   */
  @ApiMayChange
  final class EventProducerSource private (
      val entityType: String,
      val streamId: String,
      val transformation: Transformation,
      val settings: EventProducerSettings,
      val producerFilter: EventEnvelope[Any] => Boolean,
      val transformSnapshot: Option[Any => Any]) {
    require(entityType.nonEmpty, "Stream id must not be empty")
    require(streamId.nonEmpty, "Stream id must not be empty")

    def withProducerFilter[Event](producerFilter: EventEnvelope[Event] => Boolean): EventProducerSource =
      copy(producerFilter = producerFilter.asInstanceOf[EventEnvelope[Any] => Boolean])

    def withStartingFromSnapshots[Snapshot, Event](transformSnapshot: Snapshot => Event): EventProducerSource =
      copy(transformSnapshot = Some(transformSnapshot.asInstanceOf[Any => Any]))

    def copy(
        entityType: String = entityType,
        streamId: String = streamId,
        transformation: Transformation = transformation,
        settings: EventProducerSettings = settings,
        producerFilter: EventEnvelope[Any] => Boolean = producerFilter,
        transformSnapshot: Option[Any => Any] = transformSnapshot): EventProducerSource =
      new EventProducerSource(entityType, streamId, transformation, settings, producerFilter, transformSnapshot)

  }

  @ApiMayChange
  object Transformation {

    val empty: Transformation = new Transformation(
      mappers = Map.empty,
      orElse = envelope =>
        Future.failed(new IllegalArgumentException(s"Missing transformation for event [${envelope.event.getClass}]")))

    /**
     * No transformation. Pass through each event as is.
     */
    val identity: Transformation =
      new Transformation(mappers = Map.empty, orElse = envelope => Future.successful(envelope.eventOption))
  }

  /**
   * Transformation of events to the external (public) representation.
   * Events can be excluded by mapping them to `None`.
   */
  @ApiMayChange
  final class Transformation private (
      private[akka] val mappers: Map[Class[_], EventEnvelope[Any] => Future[Option[Any]]],
      private[akka] val orElse: EventEnvelope[Any] => Future[Option[Any]]) {

    /**
     * @param f A function that is fed each event, and the possible additional metadata
     */
    def registerAsyncEnvelopeMapper[A: ClassTag, B](f: EventEnvelope[A] => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(mappers.updated(clazz, f.asInstanceOf[EventEnvelope[Any] => Future[Option[Any]]]), orElse)
    }

    def registerAsyncMapper[A: ClassTag, B](f: A => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(
        mappers.updated(clazz, (envelope: EventEnvelope[Any]) => f(envelope.event.asInstanceOf[A])),
        orElse)
    }

    def registerMapper[A: ClassTag, B](f: A => Option[B]): Transformation = {
      registerAsyncMapper[A, B](event => Future.successful(f(event)))
    }

    def registerAsyncOrElseMapper(f: Any => Future[Option[Any]]): Transformation = {
      new Transformation(mappers, (envelope: EventEnvelope[Any]) => f(envelope.event))
    }

    def registerOrElseMapper(f: Any => Option[Any]): Transformation = {
      registerAsyncOrElseMapper(event => Future.successful(f(event)))
    }

    def registerAsyncEnvelopeOrElseMapper(m: EventEnvelope[Any] => Future[Option[Any]]): Transformation = {
      new Transformation(mappers, m)
    }

    private[akka] def apply(envelope: EventEnvelope[Any]): Future[Option[Any]] = {
      val mapper = mappers.getOrElse(envelope.event.getClass, orElse)
      mapper.apply(envelope)
    }

    private[akka] def toJava: JTransformation =
      new JTransformation(this)
  }

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   */
  def grpcServiceHandler(source: EventProducerSource)(
      implicit system: ActorSystem[_]): PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] =
    grpcServiceHandler(Set(source))

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   *
   * @param sources All sources that should be available from this event producer
   */
  def grpcServiceHandler(sources: Set[EventProducerSource])(
      implicit system: ActorSystem[_]): PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] = {

    grpcServiceHandler(sources, None)
  }

  /**
   * The gRPC route that can be included in an Akka HTTP server.
   *
   * @param sources All sources that should be available from this event producer
   * @param interceptor An optional request interceptor applied to each request to the service
   */
  def grpcServiceHandler(sources: Set[EventProducerSource], interceptor: Option[EventProducerInterceptor])(
      implicit system: ActorSystem[_]): PartialFunction[HttpRequest, scala.concurrent.Future[HttpResponse]] = {

    EventProducerServicePowerApiHandler.partial(
      new EventProducerServiceImpl(
        system,
        eventsBySlicesQueriesForStreamIds(sources, system),
        eventsBySlicesStartingFromSnapshotsQueriesForStreamIds(sources, system),
        currentEventsByPersistenceIdQueriesForStreamIds(sources, system),
        sources,
        interceptor))
  }

  /**
   * INTERNAL API
   */
  private[akka] def eventsBySlicesQueriesForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, EventsBySliceQuery] = {
    val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
      sources.map(s => s.streamId -> s).toMap
    queriesForStreamIds(sources, system).collect {
      case (streamId, q: EventsBySliceQuery) if streamIdToSourceMap(streamId).transformSnapshot.isEmpty =>
        streamId -> q
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def eventsBySlicesStartingFromSnapshotsQueriesForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, EventsBySliceStartingFromSnapshotsQuery] = {
    val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
      sources.map(s => s.streamId -> s).toMap
    queriesForStreamIds(sources, system).collect {
      case (streamId, q: EventsBySliceStartingFromSnapshotsQuery)
          if streamIdToSourceMap(streamId).transformSnapshot.isDefined =>
        streamId -> q
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def currentEventsByPersistenceIdQueriesForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, CurrentEventsByPersistenceIdTypedQuery] = {
    queriesForStreamIds(sources, system).collect {
      case (streamId, q: CurrentEventsByPersistenceIdTypedQuery) => streamId -> q
    }
  }

  private def queriesForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, ReadJournal] = {
    val streamIds = sources.map(_.streamId)
    require(
      streamIds.size == sources.size,
      s"EventProducerSource set contains duplicate stream id, each stream id must be unique, all stream ids: [${streamIds
        .mkString(", ")}]")

    val queryPluginsIds = sources.groupBy { eps =>
      require(
        eps.settings.queryPluginId.nonEmpty,
        s"Configuration property [akka.projection.grpc.producer.query-plugin-id] must be defined for stream id [${eps.streamId}].")
      eps.settings.queryPluginId
    }

    queryPluginsIds.flatMap {
      case (queryPluginId, sourcesUsingIt) =>
        val eventsBySlicesQuery =
          PersistenceQuery(system)
            .readJournalFor[ReadJournal](queryPluginId)

        sourcesUsingIt.map(eps => eps.streamId -> eventsBySlicesQuery)
    }
  }

}

/**
 * Interceptor allowing for example authentication/authorization of incoming requests to consume a specific stream.
 */
@ApiMayChange
trait EventProducerInterceptor {

  /**
   * Let's requests through if method returns, can fail request by throwing asynchronously failing the returned
   * future with a [[akka.grpc.GrpcServiceException]]
   *
   */
  def intercept(streamId: String, requestMetadata: Metadata): Future[Done]

}
