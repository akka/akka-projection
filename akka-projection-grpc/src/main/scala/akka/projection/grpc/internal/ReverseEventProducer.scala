/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.MetadataBuilder
import akka.projection.grpc.internal.proto.ConsumerStreamIn
import akka.projection.grpc.internal.proto.ConsumerStreamOut
import akka.projection.grpc.internal.proto.ControlCommand
import akka.projection.grpc.internal.proto.ControlStreamRequest
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.internal.proto.InitConsumerStream
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.stream.scaladsl.Keep
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

  def apply(actualEventProducer: EventProducerServiceImpl): Behavior[Command] = Behaviors.setup { context =>
    implicit val system: ActorSystem[_] = context.system
    // FIXME settings abstraction
    val config = context.system.settings.config.getConfig("akka.projection.grpc.producer")
    val producerId = config.getString("producer-id")
    val destinations = config.getConfigList("reverse-destinations")
    // One per listed upstream, with SDP?, or do we only support one?
    require(destinations.size() == 1)
    val grpcClientSettings = GrpcClientSettings.fromConfig(destinations.get(0).getConfig("client"))
    val client = EventConsumerServiceClient(grpcClientSettings)

    // FIXME restart call if connection lost
    // FIXME tie client stream lifecycle to actor
    // tail control events from the consumer
    client
      .control(ControlStreamRequest(producerId))
      .runWith(Sink.foreach { controlCommand => context.self ! Handle(controlCommand) })

    Behaviors.receiveMessagePartial {
      case Handle(ControlCommand(ControlCommand.Message.Init(InitConsumerStream(requestId, Some(initReq), _)), _)) =>
        context.log.infoN(
          "Starting reverse event stream to [{}:{}], request id [{}], for stream id [{}], slice range [{}-{}]{} (FIXME not really, yet)",
          grpcClientSettings.serviceName,
          grpcClientSettings.servicePortName,
          requestId,
          initReq.streamId,
          initReq.sliceMin,
          initReq.sliceMax,
          initReq.offset.map(o => s", offset [$o]").getOrElse(""))

        // FIXME could we separate/abstract the querying/filtering from the transformation in EventProducerServiceImpl
        //       and avoid the extra protobuf message roundtrip for every message?

        // connect client stream out to query control in
        val (controlSubscriber, controlPublisher) =
          Source.asSubscriber[ConsumerStreamOut].toMat(Sink.asPublisher(false))(Keep.both).run()
        val controlFromClient: Sink[ConsumerStreamOut, NotUsed] = Sink.fromSubscriber(controlSubscriber)
        val controlToQuery: Source[StreamIn, NotUsed] = Source.fromPublisher(controlPublisher).map {
          case ConsumerStreamOut(ConsumerStreamOut.Message.Filter(filterReq), _) =>
            StreamIn(StreamIn.Message.Filter(filterReq))
          case ConsumerStreamOut(ConsumerStreamOut.Message.Replay(replayReq), _) =>
            StreamIn(StreamIn.Message.Replay(replayReq))
          case ConsumerStreamOut(ConsumerStreamOut.Message.Empty, _) =>
            StreamIn(StreamIn.Message.Empty)
        }

        val eventsFromQuery: Source[StreamOut, NotUsed] = actualEventProducer.eventsBySlices(
          Source.single(StreamIn(StreamIn.Message.Init(initReq))).concat(controlToQuery),
          // FIXME pass along request metadata?
          MetadataBuilder.empty)

        val eventsToClient: Source[ConsumerStreamIn, NotUsed] = eventsFromQuery.map {
          case StreamOut(StreamOut.Message.Event(event), _) =>
            ConsumerStreamIn(ConsumerStreamIn.Message.Event(event))
          case StreamOut(StreamOut.Message.FilteredEvent(filter), _) =>
            ConsumerStreamIn(ConsumerStreamIn.Message.FilteredEvent(filter))
          case StreamOut(StreamOut.Message.Empty, _) =>
            ConsumerStreamIn(ConsumerStreamIn.Message.Empty)
        }

        val clientEventStreamIn: Source[ConsumerStreamIn, NotUsed] =
          Source
          // we start with the initReq so that the producer can correlate command with actual stream
            .single(ConsumerStreamIn(ConsumerStreamIn.Message.Init(InitConsumerStream(requestId, Some(initReq)))))
            .concat(eventsToClient)
        client.eventStream(clientEventStreamIn).runWith(controlFromClient)

        Behaviors.same
    }
  }

}
