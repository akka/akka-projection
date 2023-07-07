/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.javadsl

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.javadsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.grpc.internal.EventPusher
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.stream.javadsl.FlowWithContext

import java.util.Optional
import scala.compat.java8.OptionConverters.RichOptionalGeneric

/**
 * An active producer for event producer push that can be started on the producer to connect to consumers to
 * push events, for example to run a projection piercing firewalls or NAT. A producer can push events for multiple
 * entities but no two producer are allowed to push events for the same entity.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and
 * initiate connections to the producing side.
 *
 * Expects a [[akka.projection.grpc.consumer.javadsl.EventProducerPushDestination]] gRPC service
 * to be set up to accept the events on the consuming side.
 */
object EventProducerPush {
  def create[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      connectionMetadata: Metadata,
      grpcClientSettings: GrpcClientSettings): EventProducerPush[Event] =
    new EventProducerPush[Event](originId, eventProducerSource, Optional.of(connectionMetadata), grpcClientSettings)

  def create[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      grpcClientSettings: GrpcClientSettings): EventProducerPush[Event] =
    new EventProducerPush[Event](originId, eventProducerSource, Optional.empty(), grpcClientSettings)

}

final class EventProducerPush[Event] private (
    val originId: String,
    val eventProducerSource: EventProducerSource,
    val connectionMetadata: Optional[Metadata],
    val grpcClientSettings: GrpcClientSettings) {

  def handler(system: ActorSystem[_])
      : FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    val eventConsumerClient = EventConsumerServiceClient(grpcClientSettings)(system)
    val scalaMeta: akka.grpc.scaladsl.Metadata =
      connectionMetadata.asScala.map(_.asScala).getOrElse(MetadataBuilder.empty)
    EventPusher[Event](originId, eventConsumerClient, eventProducerSource.asScala, scalaMeta)(system).asJava
  }

}
