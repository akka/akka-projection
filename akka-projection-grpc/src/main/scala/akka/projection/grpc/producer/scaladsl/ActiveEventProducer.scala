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

object ActiveEventProducer {

  def apply[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      connectionMetadata: Metadata,
      grpcClientSettings: GrpcClientSettings): ActiveEventProducer[Event] =
    new ActiveEventProducer[Event](originId, eventProducerSource, Some(connectionMetadata), grpcClientSettings)

  def apply[Event](
      originId: String,
      eventProducerSource: EventProducerSource,
      grpcClientSettings: GrpcClientSettings): ActiveEventProducer[Event] =
    new ActiveEventProducer[Event](originId, eventProducerSource, None, grpcClientSettings)
}

final class ActiveEventProducer[Event](
    val originId: String,
    val eventProducerSource: EventProducerSource,
    val connectionMetadata: Option[Metadata],
    val grpcClientSettings: GrpcClientSettings) {

  def handler()(implicit system: ActorSystem[_])
      : FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    // FIXME gprc client config - use stream id to look up block? Use host to lookup port (normal gRPC client config)? Something else?
    val eventConsumerClient = EventConsumerServiceClient(grpcClientSettings)
    EventPusher(originId, eventConsumerClient, eventProducerSource, connectionMetadata.getOrElse(MetadataBuilder.empty))
  }
}
