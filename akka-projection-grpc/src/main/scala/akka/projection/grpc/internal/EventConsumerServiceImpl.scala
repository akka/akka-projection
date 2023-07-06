/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.persistence.EventWriter
import akka.persistence.EventWriterExtension
import akka.persistence.FilteredPayload
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.ConsumerEventStart
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApi
import akka.stream.scaladsl.Source
import akka.util.Timeout
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

/**
 * INTERNAL API
 *
 * gRPC push protocol service for the consuming side
 */
@InternalApi
private[akka] final class EventConsumerServiceImpl(eventProducerDestination: EventProducerPushDestination)(
    implicit system: ActorSystem[_])
    extends EventConsumerServicePowerApi {

  import ProtobufProtocolConversions._

  private val logger = LoggerFactory.getLogger(classOf[EventConsumerServiceImpl])

  private def journalPluginId = eventProducerDestination.journalPluginId
  private val eventWriter = EventWriterExtension(system).writerForJournal(journalPluginId)

  private val protoAnySerialization = new ProtoAnySerialization(system)
  private val parallelism = 1000 // FIXME config
  private implicit val timeout: Timeout = 5.seconds // FIXME from config or can we get rid of it
  private val perPartitionParallelism =
    system.settings.config.getInt("akka.persistence.event-writer.max-batch-size") / 2

  private implicit val ec: ExecutionContext = system.executionContext

  logger.info2(
    "Passive event consumer service created, accepting stream ids [{}], will write to journal [{}]",
    eventProducerDestination.acceptedStreamIds.mkString(", "),
    journalPluginId.getOrElse("default journal"))

  override def consumeEvent(
      in: Source[ConsumeEventIn, NotUsed],
      metadata: Metadata): Source[ConsumeEventOut, NotUsed] = {
    @volatile var transformer: EventProducerPushDestination.Transformation = null
    val startEvent = Promise[ConsumeEventOut]()
    in.prefixAndTail(1)
      .flatMapConcat {
        case (Seq(ConsumeEventIn(ConsumeEventIn.Message.Init(init), _)), tail) =>
          if (!eventProducerDestination.acceptedStreamIds(init.streamId)) {
            logger.debug2(
              "Event producer [{}] wanted to push events for stream id [{}] but that is not among the accepted stream ids",
              init.originId,
              init.streamId)
            throw new GrpcServiceException(Status.PERMISSION_DENIED.withDescription(
              s"Events for stream id [${init.streamId}] not accepted by this consumer"))
          }

          startEvent.success(ConsumeEventOut(
            ConsumeEventOut.Message.Start(ConsumerEventStart(toProtoFilterCriteria(eventProducerDestination.filters)))))

          val eventsAndFiltered = tail.collect {
            case c if c.message.isEvent || c.message.isFilteredEvent => c
            // keepalive consumed and dropped here
          }
          transformer = eventProducerDestination.transformationForOrigin(init.originId, metadata)

          // allow interceptor to block request based on metadata
          eventProducerDestination.interceptor match {
            case Some(interceptor) =>
              Source.futureSource(interceptor.intercept(init.streamId, metadata).map { _ =>
                logger.info2("Event stream from [{}] for stream id [{}] started", init.originId, init.streamId)
                eventsAndFiltered
              })
            case None =>
              logger.info2("Event stream from [{}] for stream id [{}] started", init.originId, init.streamId)
              eventsAndFiltered
          }

        case (_, _) =>
          throw new GrpcServiceException(Status.INVALID_ARGUMENT.withDescription(
            "Consumer stream in must start with Init event followed by events but got something else"))
      }
      .map { consumeEventIn =>
        if (consumeEventIn.message.isEvent)
          ProtobufProtocolConversions.eventToEnvelope[Any](consumeEventIn.getEvent, protoAnySerialization)
        else if (consumeEventIn.message.isFilteredEvent) {
          ProtobufProtocolConversions.filteredEventToEnvelope[Any](consumeEventIn.getFilteredEvent)
        } else {
          throw new GrpcServiceException(Status.INVALID_ARGUMENT
            .withDescription(s"Unexpected type of ConsumeEventIn: ${consumeEventIn.message.getClass}"))
        }
      }
      // FIXME config for parallelism, and perPartition (aligned with event writer batch config)
      // Note that perPartition must be something like half the siez of the event writer max buffer as the previous
      // in flight batch will complete one by one
      .mapAsyncPartitioned(parallelism, perPartitionParallelism)(_.persistenceId) { (originalEnvelope, _) =>
        val transformedEventEnvelope = transformer(originalEnvelope)
        if (logger.isTraceEnabled)
          logger.traceN(
            "Saw event [{}] for pid [{}]{}",
            transformedEventEnvelope.sequenceNr,
            transformedEventEnvelope.persistenceId,
            if (transformedEventEnvelope.filtered) " filtered" else "")

        eventWriter
          .askWithStatus[EventWriter.WriteAck](EventWriter.Write(
            transformedEventEnvelope.persistenceId,
            transformedEventEnvelope.sequenceNr,
            transformedEventEnvelope.eventOption.getOrElse(FilteredPayload),
            transformedEventEnvelope.eventMetadata,
            transformedEventEnvelope.tags,
            _))
          .map(_ =>
            // ack using the original pid in case it was transformed
            ConsumeEventOut(ConsumeEventOut.Message.Ack(
              ConsumerEventAck(originalEnvelope.persistenceId, originalEnvelope.sequenceNr))))(
            ExecutionContexts.parasitic)
          .recover {
            case NonFatal(ex) =>
              logger.warn(s"Failing event stream because of event writer error", ex)
              throw ex;
          }(system.executionContext)
      }
      .prepend(Source.future(startEvent.future))
  }

}
