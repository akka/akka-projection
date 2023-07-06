/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.grpc.scaladsl.BytesEntry
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.StreamResponseRequestBuilder
import akka.grpc.scaladsl.StringEntry
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.grpc.internal.FilterStage.Filter
import akka.projection.grpc.internal.ProtobufProtocolConversions.offsetToProtoOffset
import akka.projection.grpc.internal.ProtobufProtocolConversions.updateFilterFromProto
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventInit
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.internal.proto.FilteredEvent
import akka.projection.grpc.internal.proto.KeepAlive
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
import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.Duration

/**
 * INTERNAL API
 *
 * gRPC push protocol handler for the producing side
 */
private[akka] object EventPusher {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply[Event](
      originId: String,
      client: EventConsumerServiceClient,
      eps: EventProducerSource,
      additionalRequestMetadata: Metadata)(implicit system: ActorSystem[_])
      : FlowWithContext[EventEnvelope[Event], ProjectionContext, Done, ProjectionContext, NotUsed] = {
    import akka.projection.grpc.internal.ProtobufProtocolConversions.transformAndEncodeEvent

    implicit val ec: ExecutionContext = system.executionContext
    val protoAnySerialization = new ProtoAnySerialization(system)

    def filterAndTransformFlow(filters: Future[immutable.Seq[proto.FilterCriteria]])
        : Flow[(EventEnvelope[Event], ProjectionContext), (ConsumeEventIn, ProjectionContext), NotUsed] =
      Flow
        .futureFlow(filters.map { filterCriteria =>
          val consumerFilter =
            updateFilterFromProto(
              Filter.empty(eps.settings.topicTagPrefix),
              filterCriteria,
              // FIXME not replicated, so maybe this is safe?
              mapEntityIdToPidHandledByThisStream = identity)

          Flow[(EventEnvelope[Event], ProjectionContext)]
            .mapAsync(eps.settings.transformationParallelism) {
              case (envelope, projectionContext) =>
                val filteredTransformed =
                  if (eps.producerFilter(envelope.asInstanceOf[EventEnvelope[Any]]) &&
                      consumerFilter.matches(envelope)) {
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
                  case Some(protoEvent) => (ConsumeEventIn(ConsumeEventIn.Message.Event(protoEvent)), projectionContext)
                  case None             =>
                    // Filtered or transformed to None, we still need to push a placeholder to not get seqnr gaps on the receiving side
                    (
                      ConsumeEventIn(
                        ConsumeEventIn.Message.FilteredEvent(
                          FilteredEvent(
                            persistenceId = envelope.persistenceId,
                            seqNr = envelope.sequenceNr,
                            slice = envelope.slice,
                            offset = offsetToProtoOffset(envelope.offset)))),
                      projectionContext)
                }
            }
        })
        .mapMaterializedValue(_ => NotUsed)

    FlowWithContext
      .fromTuples(
        Flow
          .fromMaterializer { (_, _) =>
            val topicFiltersPromise = Promise[immutable.Seq[proto.FilterCriteria]]()

            Flow[(EventEnvelope[Event], ProjectionContext)]
              .via(filterAndTransformFlow(topicFiltersPromise.future))
              .via(
                if (eps.settings.keepAliveInterval != Duration.Zero)
                  Flow[(proto.ConsumeEventIn, ProjectionContext)]
                    .keepAlive(eps.settings.keepAliveInterval, () => KeepAliveTuple)
                else
                  Flow[(proto.ConsumeEventIn, ProjectionContext)])
              .via(Flow.fromGraph(
                new EventPusherStage(originId, eps, client, additionalRequestMetadata, topicFiltersPromise)))

          }
          .mapMaterializedValue(_ => NotUsed))
  }

  private[internal] val KeepAliveTuple: (proto.ConsumeEventIn, ProjectionContext) =
    (ConsumeEventIn(ConsumeEventIn.Message.KeepAlive(KeepAlive.defaultInstance)), null)
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] class EventPusherStage(
    originId: String,
    eps: EventProducerSource,
    client: EventConsumerServiceClient,
    additionalRequestMetadata: Metadata,
    topicFilterPromise: Promise[immutable.Seq[proto.FilterCriteria]])
    extends GraphStage[FlowShape[(ConsumeEventIn, ProjectionContext), (Done, ProjectionContext)]] {
  import EventPusher.KeepAliveTuple

  val in = Inlet[(ConsumeEventIn, ProjectionContext)]("EventPusherStage.in")
  val out = Outlet[(Done, ProjectionContext)]("EventPusherStage.out")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private val inFlight = new util.HashMap[(String, Long), ProjectionContext]()

    // hold off pushing events until we saw start response message
    private var waitingForStart: Boolean = true

    private val toConsumer: SubSourceOutlet[proto.ConsumeEventIn] = new SubSourceOutlet("EventPusherStage.toConsumer")
    private val fromConsumer: SubSinkInlet[proto.ConsumeEventOut] = new SubSinkInlet("EventPusherStage.fromConsumer")

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        tryGrabInAndPushToClient()
      }
    })
    toConsumer.setHandler(new OutHandler {
      override def onPull(): Unit =
        pull(in)
      override def onDownstreamFinish(cause: Throwable): Unit = cancel(in, cause)
    })
    fromConsumer.setHandler(new InHandler {
      override def onPush(): Unit = {
        val eventOut = fromConsumer.grab()
        eventOut match {
          case ConsumeEventOut(ConsumeEventOut.Message.Ack(eventAck), _) =>
            val key = (eventAck.persistenceId, eventAck.seqNr)
            val context = inFlight.get(key)
            if (context eq null) throw new IllegalStateException(s"Saw ack for $key but in inFlight tracker map")
            inFlight.remove(key)
            push(out, (Done, context))
          case ConsumeEventOut(ConsumeEventOut.Message.Start(start), _) =>
            waitingForStart = false
            topicFilterPromise.trySuccess(start.filter.toVector)
            tryGrabInAndPushToClient()
            fromConsumer.pull()
          case unexpected =>
            throw new IllegalArgumentException(s"Unexpected ConsumeEventOut message: ${unexpected.getClass}")
        }

      }

      override def onUpstreamFinish(): Unit = complete(out)
      override def onUpstreamFailure(ex: Throwable): Unit = failStage(ex)
    })
    setHandler(out, new OutHandler {
      override def onPull(): Unit = fromConsumer.pull()
    })

    def tryGrabInAndPushToClient(): Unit = {
      if (!waitingForStart && isAvailable(in)) {
        grab(in) match {
          case KeepAliveTuple =>
            // no keep track of context for these
            toConsumer.push(KeepAliveTuple._1)
          case (event, context) =>
            val key =
              event.message match {
                case ConsumeEventIn.Message.Event(evt)              => (evt.persistenceId, evt.seqNr)
                case ConsumeEventIn.Message.FilteredEvent(filtered) => (filtered.persistenceId, filtered.seqNr)
                case unexpected =>
                  throw new IllegalArgumentException(s"Unexpected ConsumeMessageIn: ${unexpected.getClass}")
              }
            inFlight.put(key, context)
            toConsumer.push(event)
        }
      }
    }

    override def preStart(): Unit = {
      addRequestHeaders(
        client
          .consumeEvent())
        .invokeWithMetadata(
          Source
            .single(ConsumeEventIn(
              ConsumeEventIn.Message.Init(ConsumerEventInit(originId = originId, streamId = eps.streamId))))
            .concat(Source.fromGraph(toConsumer.source)))
        .runWith(Sink.fromGraph(fromConsumer.sink))(materializer)
    }

    private def addRequestHeaders[Req, Res](
        builder: StreamResponseRequestBuilder[Req, Res]): StreamResponseRequestBuilder[Req, Res] = {
      val additionalRequestHeaders = additionalRequestMetadata.asList
      additionalRequestHeaders.foldLeft(builder) {
        case (acc, (key, StringEntry(str)))  => acc.addHeader(key, str)
        case (acc, (key, BytesEntry(bytes))) => acc.addHeader(key, bytes)
      }
    }

    override def postStop(): Unit = {
      topicFilterPromise.tryFailure(new RuntimeException("Stage stopped before getting a start message from consumer"))
    }
  }
}
