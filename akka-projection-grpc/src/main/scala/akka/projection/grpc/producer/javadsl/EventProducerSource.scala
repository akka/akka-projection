/*
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.annotation.ApiMayChange
import akka.persistence.query.typed.EventEnvelope
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
    val producerFilter: java.util.function.Predicate[EventEnvelope[Any]]) {

  def this(entityType: String, streamId: String, transformation: Transformation, settings: EventProducerSettings) =
    this(entityType, streamId, transformation, settings, producerFilter = _ => true)

  def withProducerFilter[Event](
      producerFilter: java.util.function.Predicate[EventEnvelope[Event]]): EventProducerSource =
    new EventProducerSource(
      entityType,
      streamId,
      transformation,
      settings,
      producerFilter.asInstanceOf[java.util.function.Predicate[EventEnvelope[Any]]])

  def asScala: akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource =
    akka.projection.grpc.producer.scaladsl.EventProducer
      .EventProducerSource(entityType, streamId, transformation.delegate, settings, producerFilter.test)
}
