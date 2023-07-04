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
