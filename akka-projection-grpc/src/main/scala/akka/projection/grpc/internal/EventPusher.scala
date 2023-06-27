/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.grpc.internal.ProtobufProtocolConversions.offsetToProtoOffset
import akka.projection.grpc.internal.proto.ConsumerEvent
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.ConsumerEventInit
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.scaladsl.Handler
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * INTERNAL API
 *
 * gRPC push protocol handler for the producing side
 */
private[akka] class EventPusher[Event](client: EventConsumerServiceClient, eps: EventProducerSource)(
    implicit system: ActorSystem[_]) {
  import akka.projection.grpc.internal.ProtobufProtocolConversions.transformAndEncodeEvent

  private val logger = LoggerFactory.getLogger(classOf[EventPusher[_]])
  private implicit val ec: ExecutionContext = system.executionContext
  private val protoAnySerialization = new ProtoAnySerialization(system)
  private val originId = "fixme"

  def flow: FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    FlowWithContext[EventEnvelope[Event], ProjectionContext]
      .mapAsync(1) { envelope =>
        val filteredTransformed =
          if (eps.producerFilter(envelope.asInstanceOf[EventEnvelope[Any]])) {
            if (logger.isTraceEnabled())
              logger.trace(
                "Pushing event persistence id [{}], sequence number [{}]",
                envelope.persistenceId,
                envelope.sequenceNr)

            transformAndEncodeEvent(eps.transformation, envelope, protoAnySerialization)
          } else {
            if (logger.isTraceEnabled())
              logger.trace(
                "Filtering event persistence id [{}], sequence number [{}]",
                envelope.persistenceId,
                envelope.sequenceNr)

            Future.successful(None)
          }

        filteredTransformed.map {
          case Some(protoEvent) => protoEvent
          case None             =>
            // Filtered or transformed to None, we still need to push a placeholder to not get seqnr gaps on the receiving side
            proto.Event(
              persistenceId = envelope.persistenceId,
              seqNr = envelope.sequenceNr,
              slice = envelope.slice,
              offset = offsetToProtoOffset(envelope.offset),
              payload = None,
              tags = Seq.empty)
        }
      }
      .via(Flow.lazyFlow { () =>
        // FIXME optimally akka-grpc client tools for this would be:
        //    client.consumeEventFlow: Flow[In, Out]
        //    client.consumeEventFlowWithContext: FlowWithContext[In, Ctx, Out, Ctx, NotUsed]
        // A bit messy because we need to turn the source in out of the gRPC client into a flow
        // and allow multiple materializations
        val (eventPublisher, eventSink) =
          Sink.asPublisher[EventEnvelope[Event]](fanout = false).preMaterialize()

        val (responseSubscriber, responseSource) = Source
          .asSubscriber[ConsumerEventAck]
          .preMaterialize()

        client
          .consumeEvent(
            Source
              .single(ConsumerEvent(ConsumerEvent.Message.Init(ConsumerEventInit(originId))))
              .concat(Source.fromPublisher(eventPublisher)))
          .runWith(Sink.fromSubscriber(responseSubscriber))

        // argh, how do we pass context, can we recreate?
        Flow.fromSinkAndSource(eventSink, responseSource).asFlowWithContext((evt, ctx) => ???)(consumerEventAck => ???)
      })
  }
}
