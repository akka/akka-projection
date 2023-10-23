/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import java.util.Optional

import akka.annotation.ApiMayChange
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.TopicMatcher
import akka.projection.grpc.producer.EventProducerSettings

/**
 * @param entityType The internal entity type name
 * @param streamId The public, logical, stream id that consumers use to consume this source
 * @param transformation Transformations for turning the internal events to public message types
 * @param settings The event producer settings used (can be shared for multiple sources)
 */
@ApiMayChange
final class EventProducerSource(
    val entityType: String,
    val streamId: String,
    val transformation: Transformation,
    val settings: EventProducerSettings,
    val producerFilter: java.util.function.Predicate[EventEnvelope[Any]],
    val transformSnapshot: Optional[java.util.function.Function[Any, Any]]) {

  def this(entityType: String, streamId: String, transformation: Transformation, settings: EventProducerSettings) =
    this(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter = _ => true,
      Optional.empty[java.util.function.Function[Any, Any]]())

  def this(
      entityType: String,
      streamId: String,
      transformation: Transformation,
      settings: EventProducerSettings,
      producerFilter: java.util.function.Predicate[EventEnvelope[Any]]) =
    this(entityType, streamId, transformation, settings, producerFilter, Optional.empty())

  /**
   * Filter events matching the predicate, for example based on tags.
   */
  def withProducerFilter[Event](
      producerFilter: java.util.function.Predicate[EventEnvelope[Event]]): EventProducerSource =
    new EventProducerSource(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter.asInstanceOf[java.util.function.Predicate[EventEnvelope[Any]]],
      Optional.empty())

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
  def withStartingFromSnapshots[Snapshot, Event](transformSnapshot: java.util.function.Function[Snapshot, Event]) =
    new EventProducerSource(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter,
      Optional.of(transformSnapshot.asInstanceOf[java.util.function.Function[Any, Any]]))

  def asScala: akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource = {
    val scalaEventProducer =
      akka.projection.grpc.producer.scaladsl.EventProducer
        .EventProducerSource(entityType, streamId, transformation.delegate, settings, producerFilter.test)
    if (transformSnapshot.isPresent) scalaEventProducer.withStartingFromSnapshots(transformSnapshot.get.apply(_))
    else scalaEventProducer

  }
}
