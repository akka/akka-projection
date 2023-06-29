/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.grpc.internal.ProtobufProtocolConversions.offsetToProtoOffset
import akka.projection.grpc.internal.proto.ConsumerEvent
import akka.projection.grpc.internal.proto.ConsumerEventInit
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import org.slf4j.LoggerFactory

import java.util
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 *
 * gRPC push protocol handler for the producing side
 */
private[akka] object EventPusher {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply[Event](originId: String, client: EventConsumerServiceClient, eps: EventProducerSource)(
      implicit system: ActorSystem[_])
      : FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    import akka.projection.grpc.internal.ProtobufProtocolConversions.transformAndEncodeEvent

    implicit val ec: ExecutionContext = system.executionContext
    val protoAnySerialization = new ProtoAnySerialization(system)

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
      .via(Flow.fromGraph(new EventPusherStage(originId, eps, client)))

  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class EventPusherStage(originId: String, eps: EventProducerSource, client: EventConsumerServiceClient)
    extends GraphStage[FlowShape[(proto.Event, ProjectionContext), (Done, ProjectionContext)]] {

  val in = Inlet[(proto.Event, ProjectionContext)]("EventPusherStage.in")
  val out = Outlet[(Done, ProjectionContext)]("EventPusherStage.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val inFlight = new util.HashMap[(String, Long), ProjectionContext]()

    private val toConsumer: SubSourceOutlet[proto.ConsumerEvent] = new SubSourceOutlet("EventPusherStage.toConsumer")
    private val fromConsumer: SubSinkInlet[proto.ConsumerEventAck] = new SubSinkInlet("EventPusherStage.fromConsumer")

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val (event, context) = grab(in)
        val key = (event.persistenceId, event.seqNr)
        inFlight.put(key, context)
        toConsumer.push(ConsumerEvent(ConsumerEvent.Message.Event(event)))
      }
    })
    toConsumer.setHandler(new OutHandler {
      override def onPull(): Unit =
        pull(in)
      override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
    })
    fromConsumer.setHandler(new InHandler {
      override def onPush(): Unit = {
        val eventAck = fromConsumer.grab()
        val key = (eventAck.persistenceId, eventAck.seqNr)
        val context = inFlight.get(key)
        if (context eq null) throw new IllegalStateException(s"Saw ack for $key but in inFlight tracker map")
        inFlight.remove(key)
        push(out, (Done, context))
      }

      override def onUpstreamFinish(): Unit = complete(out)
      override def onUpstreamFailure(ex: Throwable): Unit = failStage(ex)
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = fromConsumer.pull()
    })

    override def preStart(): Unit = {
      client
        .consumeEvent(
          Source
            .single(ConsumerEvent(
              ConsumerEvent.Message.Init(ConsumerEventInit(originId = originId, streamId = eps.streamId))))
            .concat(Source.fromGraph(toConsumer.source)))
        .runWith(Sink.fromGraph(fromConsumer.sink))(materializer)
    }
  }
}
