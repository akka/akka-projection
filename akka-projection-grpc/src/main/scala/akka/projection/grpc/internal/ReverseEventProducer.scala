/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.projection.grpc.internal.proto.ConsumerStreamIn
import akka.projection.grpc.internal.proto.ControlCommand
import akka.projection.grpc.internal.proto.ControlStreamRequest
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 *
 * Optionally runs on the producing side, connecting to an upstream gRPC projection event consumer.
 */
@InternalApi private[akka] object ReverseEventProducer {

  sealed trait Command
  final case class Handle(control: ControlCommand) extends Command

  def apply(): Behavior[Command] = Behaviors.setup { context =>
    implicit val system: ActorSystem[_] = context.system
    // FIXME settings abstraction
    val config = context.system.settings.config.getConfig("akka.projection.grpc.producer")
    val producerId = config.getString("producer-id")
    val destinations = config.getConfigList("reverse-destinations")
    // One per listed upstream, with SDP?, or do we only support one?
    require(destinations.size() == 1)
    val grpcClientSettings = GrpcClientSettings.fromConfig(destinations.get(0).getConfig("client"))
    val client = EventConsumerServiceClient(grpcClientSettings)

    client
      .control(ControlStreamRequest(producerId))
      .runWith(Sink.foreach { controlCommand => context.self ! Handle(controlCommand) })

    Behaviors.receiveMessage {
      case Handle(ControlCommand(ControlCommand.Message.Init(initReq), _)) =>
        context.log.infoN(
          "Starting reverse event stream to [{}:{}] for stream id [{}], slice range [{}-{}]{} (FIXME not really, yet)",
          grpcClientSettings.serviceName,
          grpcClientSettings.servicePortName,
          initReq.streamId,
          initReq.sliceMin,
          initReq.sliceMax,
          initReq.offset.map(o => s", offset [$o]").getOrElse(""))
        // FIXME how do we actually do it?
        // FIXME do we start with the initReq so that the producer can correlate command with actual stream?
        client.eventStream(Source.never[ConsumerStreamIn]).runWith(Sink.ignore /* ConsumerStreamOut */ )

        Behaviors.same

      case Handle(ControlCommand(ControlCommand.Message.Empty, _)) =>
        context.log.debug("Ignoring empty command")
        Behaviors.same
    }
  }

}
