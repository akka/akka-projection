/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import scala.concurrent.Future
import scala.reflect.ClassTag
import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.grpc.scaladsl.Metadata
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl.ReadJournal
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdStartingFromSnapshotQuery
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceQuery
import akka.persistence.query.typed.scaladsl.EventsBySliceStartingFromSnapshotsQuery
import akka.projection.grpc.internal.EventProducerServiceImpl
import akka.projection.grpc.internal.TopicMatcher
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
    require(entityType.nonEmpty, "Entity type must not be empty")
    require(streamId.nonEmpty, "Stream id must not be empty")

    /**
     * Filter events matching the predicate, for example based on tags.
     */
    def withProducerFilter[Event](producerFilter: EventEnvelope[Event] => Boolean): EventProducerSource =
      copy(producerFilter = producerFilter.asInstanceOf[EventEnvelope[Any] => Boolean])

    /**
     * Filter events matching the topic expression according to MQTT specification, including wildcards.
     * The topic of an event is defined by a tag with certain prefix, see `topic-tag-prefix` configuration.
     */
    def withTopicProducerFilter(topicExpression: String): EventProducerSource = {
      val topicMatcher = TopicMatcher(topicExpression)
      withProducerFilter[Any](topicMatcher.matches(_, settings.topicTagPrefix))
    }

    /**
     * Use snapshots as starting points and thereby reducing number of events that have to be loaded.
     * This can be useful if the consumer start from zero without any previously processed
     * offset or if it has been disconnected for a long while and its offset is far behind.
     *
     * First it loads all snapshots with timestamps greater than or equal to the offset timestamp. There is at most one
     * snapshot per persistenceId. The snapshots are transformed to events with the given `transformSnapshot` function.
     *
     * After emitting the snapshot events the ordinary events with sequence numbers after the snapshots are emitted.
     *
     * Important note: This should not be used together with [[EventProducerPush]]. In that case `SourceProvider` with
     * `eventsBySlicesStartingFromSnapshots` should be used instead.
     */
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

    /**
     * Starting point for building `Transformation`. Registrations of actual transformations must
     * be added. Use [[Transformation.identity]] to pass through each event as is.
     */
    val empty: Transformation = new Transformation(
      mappers = Map.empty,
      orElse = envelope =>
        Future.failed(
          new IllegalArgumentException(
            s"Missing transformation for event [${envelope.eventOption.map(_.getClass).getOrElse("")}]. " +
            "Use Transformation.identity to pass through each event as is.")))

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
     * @param f A function that is fed each event envelope where the payload is of type `A` and returns an
     *          async payload to emit, or `None` to filter the event from being produced.
     */
    def registerAsyncEnvelopeMapper[A: ClassTag, B](f: EventEnvelope[A] => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(mappers.updated(clazz, f.asInstanceOf[EventEnvelope[Any] => Future[Option[Any]]]), orElse)
    }

    /**
     * @param f A function that is fed each event payload of type `A` and returns an
     *          async payload to emit, or `None` to filter the event from being produced.
     */
    def registerAsyncMapper[A: ClassTag, B](f: A => Future[Option[B]]): Transformation = {
      val clazz = implicitly[ClassTag[A]].runtimeClass
      new Transformation(
        mappers.updated(clazz, (envelope: EventEnvelope[Any]) => f(envelope.event.asInstanceOf[A])),
        orElse)
    }

    /**
     * @param f A function that is fed each event payload of type `A` and returns a
     *          payload to emit, or `None` to filter the event from being produced.
     */
    def registerMapper[A: ClassTag, B](f: A => Option[B]): Transformation = {
      registerAsyncMapper[A, B](event => Future.successful(f(event)))
    }

    /**
     * @param f A function that is fed each event envelope where the payload is of type `A` and returns a
     *          payload to emit, or `None` to filter the event from being produced.
     */
    def registerEnvelopeMapper[A: ClassTag, B](f: EventEnvelope[A] => Option[B]): Transformation = {
      registerAsyncEnvelopeMapper[A, B](event => Future.successful(f(event)))
    }

    /**
     * @param f A function that is fed each event payload, that did not match any other registered mappers, returns an
     *          async payload to emit, or `None` to filter the event from being produced. Replaces any previous "orElse"
     *          mapper defined.
     */
    def registerAsyncOrElseMapper(f: Any => Future[Option[Any]]): Transformation = {
      new Transformation(mappers, (envelope: EventEnvelope[Any]) => f(envelope.event))
    }

    /**
     * @param f A function that is fed each event payload, that did not match any other registered mappers, returns a
     *          payload to emit, or `None` to filter the event from being produced. Replaces any previous "orElse"
     *          mapper defined.
     */
    def registerOrElseMapper(f: Any => Option[Any]): Transformation = {
      registerAsyncOrElseMapper(event => Future.successful(f(event)))
    }

    /**
     * @param m A function that is fed each event envelope, that did not match any other registered mappers, returns a
     *          payload to emit, or `None` to filter the event from being produced. Replaces any previous "orElse"
     *          mapper defined.
     */
    def registerAsyncEnvelopeOrElseMapper(m: EventEnvelope[Any] => Future[Option[Any]]): Transformation = {
      new Transformation(mappers, m)
    }

    /**
     * INTERNAL API
     */
    @InternalApi
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
        eventsBySlicesForStreamIds(sources, system),
        eventsBySlicesStartingFromSnapshotsForStreamIds(sources, system),
        currentEventsByPersistenceIdForStreamIds(sources, system),
        currentEventsByPersistenceIdStartingFromSnapshotForStreamIds(sources, system),
        sources,
        interceptor))
  }

  /**
   * INTERNAL API
   */
  private[akka] def eventsBySlicesForStreamIds(
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
  private[akka] def eventsBySlicesStartingFromSnapshotsForStreamIds(
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
  private[akka] def currentEventsByPersistenceIdForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, CurrentEventsByPersistenceIdTypedQuery] = {
    val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
      sources.map(s => s.streamId -> s).toMap
    queriesForStreamIds(sources, system).collect {
      case (streamId, q: CurrentEventsByPersistenceIdTypedQuery)
          if streamIdToSourceMap(streamId).transformSnapshot.isEmpty =>
        streamId -> q
    }
  }

  /**
   * INTERNAL API
   */
  private[akka] def currentEventsByPersistenceIdStartingFromSnapshotForStreamIds(
      sources: Set[EventProducerSource],
      system: ActorSystem[_]): Map[String, CurrentEventsByPersistenceIdStartingFromSnapshotQuery] = {
    val streamIdToSourceMap: Map[String, EventProducer.EventProducerSource] =
      sources.map(s => s.streamId -> s).toMap
    queriesForStreamIds(sources, system).collect {
      case (streamId, q: CurrentEventsByPersistenceIdStartingFromSnapshotQuery)
          if streamIdToSourceMap(streamId).transformSnapshot.isDefined =>
        streamId -> q
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
