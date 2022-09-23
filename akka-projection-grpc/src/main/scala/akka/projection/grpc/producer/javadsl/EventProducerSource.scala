/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.projection.grpc.producer.EventProducerSettings

/**
 * @param entityType The internal entity type name
 * @param streamId The public, logical, stream id that consumers use to consume this source
 * @param transformation Transformations for turning the internal events to public message types
 * @param settings The event producer settings used (can be shared for multiple sources)
 */
final class EventProducerSource(
    entityType: String,
    streamId: String,
    transformation: Transformation,
    settings: EventProducerSettings) {

  def asScala: akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource =
    akka.projection.grpc.producer.scaladsl.EventProducer
      .EventProducerSource(entityType, streamId, transformation.delegate, settings)
}
