/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import java.util.Optional
import java.util.function.{ Function => JFunction }
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.TopicMatcher
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.internal.EventOriginFilter

import scala.compat.java8.OptionConverters.RichOptionForJava8

object EventProducerSource {

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def fromScala(
      source: akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource): EventProducerSource =
    new EventProducerSource(
      source.entityType,
      source.streamId,
      source.transformation.toJava,
      source.settings,
      source.producerFilter.apply,
      source.transformSnapshot.map[JFunction[Any, Any]](_.apply).asJava,
      source.replicatedEventOriginFilter)
}

/**
 * @param entityType The internal entity type name
 * @param streamId The public, logical, stream id that consumers use to consume this source
 * @param transformation Transformations for turning the internal events to public message types
 * @param settings The event producer settings used (can be shared for multiple sources)
 */
final class EventProducerSource(
    val entityType: String,
    val streamId: String,
    val transformation: Transformation,
    val settings: EventProducerSettings,
    val producerFilter: java.util.function.Predicate[EventEnvelope[Any]],
    val transformSnapshot: Optional[JFunction[Any, Any]],
    @InternalApi
    private[akka] val replicatedEventOriginFilter: Option[EventOriginFilter]) {

  // FIXME replace constructor overloads with "create" factories instead?
  def this(
      entityType: String,
      streamId: String,
      transformation: Transformation,
      settings: EventProducerSettings,
      transformSnapshot: Optional[JFunction[Any, Any]]) =
    this(entityType, streamId, transformation, settings, producerFilter = _ => true, transformSnapshot, None)

  def this(entityType: String, streamId: String, transformation: Transformation, settings: EventProducerSettings) =
    this(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter = _ => true,
      Optional.empty[JFunction[Any, Any]](),
      None)

  def this(
      entityType: String,
      streamId: String,
      transformation: Transformation,
      settings: EventProducerSettings,
      producerFilter: java.util.function.Predicate[EventEnvelope[Any]]) =
    this(entityType, streamId, transformation, settings, producerFilter, Optional.empty(), None)

  /**
   * Filter events matching the predicate, for example based on tags.
   */
  def withProducerFilter[Event](
      producerFilter: java.util.function.Predicate[EventEnvelope[Event]]): EventProducerSource =
    copy(producerFilter = producerFilter.asInstanceOf[java.util.function.Predicate[EventEnvelope[Any]]])

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
  def withStartingFromSnapshots[Snapshot, Event](transformSnapshot: JFunction[Snapshot, Event]): EventProducerSource =
    copy(transformSnapshot = Optional.of(transformSnapshot.asInstanceOf[JFunction[Any, Any]]))

  /** INTERNAL API */
  @InternalApi
  private[akka] def withReplicationOriginFilter(eventOriginFilter: EventOriginFilter): EventProducerSource =
    copy(replicatedEventOriginFilter = Some(eventOriginFilter))

  private def copy(
      entityType: String = entityType,
      streamId: String = streamId,
      transformation: Transformation = transformation,
      settings: EventProducerSettings = settings,
      producerFilter: java.util.function.Predicate[EventEnvelope[Any]] = producerFilter,
      transformSnapshot: Optional[JFunction[Any, Any]] = transformSnapshot,
      replicatedEventOriginFilter: Option[EventOriginFilter] = replicatedEventOriginFilter): EventProducerSource =
    new EventProducerSource(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter,
      transformSnapshot,
      replicatedEventOriginFilter)

  def asScala: akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource = {
    val scalaEventProducer =
      akka.projection.grpc.producer.scaladsl.EventProducer
        .EventProducerSource(entityType, streamId, transformation.delegate, settings, producerFilter.test)
    val s1 =
      if (transformSnapshot.isPresent) scalaEventProducer.withStartingFromSnapshots(transformSnapshot.get.apply(_))
      else scalaEventProducer

    replicatedEventOriginFilter.fold(s1)(s1.withReplicatedEventOriginFilter)
  }
}
