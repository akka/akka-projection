/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer.scaladsl

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.grpc.internal.EventPusher
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.stream.scaladsl.FlowWithContext

/**
 * An active producer for event producer push that can be started on the producer to connect to consumers to
 * push events, for example to run a projection piercing firewalls or NAT. A producer can push events for multiple
 * entities but no two producer are allowed to push events for the same entity.
 *
 * The event consumer service is not needed for normal projections over gRPC where the consuming side can access and
 * initiate connections to the producing side.
 *
 * Expects a [[akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination]] gRPC service
 * to be set up to accept the events on the consuming side.
 */
object EventProducerPush {

  def apply[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      connectionMetadata: Metadata,
      grpcClientSettings: GrpcClientSettings): EventProducerPush[Event] =
    new EventProducerPush[Event](originId, eventProducerSource, Some(connectionMetadata), grpcClientSettings)

  def apply[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      grpcClientSettings: GrpcClientSettings): EventProducerPush[Event] =
    new EventProducerPush[Event](originId, eventProducerSource, None, grpcClientSettings)
}

/**
 * @param originId unique producer identifier showing where the events came from/was produced
 * @param connectionMetadata Additional metadata to pass to the consumer when connecting
 * @param grpcClientSettings Where to connect and publish the events, must have a [[EventProducerPush]] service listening
 */
final class EventProducerPush[Event](
    val originId: String,
    val eventProducerSource: EventProducerSource,
    val connectionMetadata: Option[Metadata],
    val grpcClientSettings: GrpcClientSettings) {

  if (eventProducerSource.transformSnapshot.isDefined)
    throw new IllegalArgumentException(
      "`EventProducerSource.withStartingFromSnapshots` should not be used together with `EventProducerPush`. " +
      "In that case `SourceProvider` with `eventsBySlicesStartingFromSnapshots` should be used instead.")

  def handler()(implicit system: ActorSystem[_])
      : FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    val eventConsumerClient = EventConsumerServiceClient(grpcClientSettings)
    EventPusher(originId, eventConsumerClient, eventProducerSource, connectionMetadata.getOrElse(MetadataBuilder.empty))
  }
}
