/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.persistence.FilteredPayload
import akka.persistence.typed.internal.EventWriter
import akka.persistence.typed.internal.EventWriterExtension
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.ConsumerEventStart
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApi
import akka.stream.scaladsl.Source
import io.grpc.Status
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.Promise

import akka.persistence.query.typed.EventEnvelope

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventPusherConsumerServiceImpl {
  // See akka.persistence.r2dbc.internal.EnvelopeOrigin, but we don't have a dependency
  // to akka-persistence-r2dbc here
  def fromSnapshot(env: EventEnvelope[_]): Boolean =
    env.source == "SN"

}

/**
 * INTERNAL API
 *
 * gRPC push protocol service for the consuming side
 */
@InternalApi
private[akka] final class EventPusherConsumerServiceImpl(
    eventProducerDestinations: Set[EventProducerPushDestination],
    preferProtobuf: ProtoAnySerialization.Prefer)(implicit system: ActorSystem[_])
    extends EventConsumerServicePowerApi {

  import EventPusherConsumerServiceImpl.fromSnapshot
  import ProtobufProtocolConversions._

  private val logger = LoggerFactory.getLogger(classOf[EventPusherConsumerServiceImpl])

  private case class Destination(
      eventProducerPushDestination: EventProducerPushDestination,
      eventWriter: ActorRef[EventWriter.Command])

  private val destinationPerStreamId = eventProducerDestinations.map { d =>
    d.acceptedStreamId -> Destination(d, EventWriterExtension(system).writerForJournal(d.journalPluginId))
  }.toMap

  private val protoAnySerialization =
    new ProtoAnySerialization(system, eventProducerDestinations.flatMap(_.protobufDescriptors).toVector, preferProtobuf)
  private val perPartitionParallelism =
    system.settings.config.getInt("akka.persistence.typed.event-writer.max-batch-size") / 2

  private implicit val ec: ExecutionContext = system.executionContext

  logger.info(
    "Passive event consumer service created, accepting stream ids [{}]",
    destinationPerStreamId.keys.mkString(", "))

  override def consumeEvent(
      in: Source[ConsumeEventIn, NotUsed],
      metadata: Metadata): Source[ConsumeEventOut, NotUsed] = {
    in.prefixAndTail(1)
      .flatMapConcat {
        case (Seq(ConsumeEventIn(ConsumeEventIn.Message.Init(init), _)), tail) =>
          val startEvent = Promise[ConsumeEventOut]()
          destinationPerStreamId.get(init.streamId) match {
            case Some(destination) =>
              startEvent.success(
                ConsumeEventOut(ConsumeEventOut.Message.Start(
                  ConsumerEventStart(toProtoFilterCriteria(destination.eventProducerPushDestination.filters)))))

              val eventsAndFiltered = tail.collect {
                case c if c.message.isEvent || c.message.isFilteredEvent => c
                // keepalive consumed and dropped here
              }
              val transformer =
                destination.eventProducerPushDestination.transformationForOrigin(init.originId, metadata)
              if (transformer eq EventProducerPushDestination.Transformation.empty)
                throw new IllegalArgumentException(
                  s"Transformation must not be empty. Use Transformation.identity to pass through each event as is.")

              // allow interceptor to block request based on metadata
              val interceptedTail = destination.eventProducerPushDestination.interceptor match {
                case Some(interceptor) =>
                  Source.futureSource(interceptor.intercept(init.streamId, metadata).map { _ =>
                    logger.info2("Event stream from [{}] for stream id [{}] started", init.originId, init.streamId)
                    eventsAndFiltered
                  })
                case None =>
                  logger.info2("Event stream from [{}] for stream id [{}] started", init.originId, init.streamId)
                  eventsAndFiltered
              }

              interceptedTail
                .map { consumeEventIn =>
                  if (consumeEventIn.message.isEvent) {
                    // When no transformation we don't need to deserialize the event and can use SerializedEvent
                    val deserializeEvent = transformer ne EventProducerPushDestination.Transformation.empty
                    ProtobufProtocolConversions.eventToEnvelope(
                      consumeEventIn.getEvent,
                      protoAnySerialization,
                      deserializeEvent)
                  } else if (consumeEventIn.message.isFilteredEvent) {
                    ProtobufProtocolConversions.filteredEventToEnvelope[Any](consumeEventIn.getFilteredEvent)
                  } else {
                    throw new GrpcServiceException(Status.INVALID_ARGUMENT
                      .withDescription(s"Unexpected type of ConsumeEventIn: ${consumeEventIn.message.getClass}"))
                  }
                }
                .mapAsyncPartitioned(
                  destination.eventProducerPushDestination.settings.parallelism,
                  perPartitionParallelism)(_.persistenceId) { (originalEnvelope, _) =>
                  val transformedEventEnvelope = transformer(originalEnvelope)

                  if (logger.isTraceEnabled)
                    logger.traceN(
                      "Saw event [{}] for pid [{}]{}",
                      transformedEventEnvelope.sequenceNr,
                      transformedEventEnvelope.persistenceId,
                      if (transformedEventEnvelope.filtered) " filtered" else "")

                  // Note that when there is no Transformation the event is SerializedEvent, which will be passed
                  // through the EventWriter and the journal can write that without additional serialization.

                  destination.eventWriter
                    .askWithStatus[EventWriter.WriteAck](EventWriter.Write(
                      transformedEventEnvelope.persistenceId,
                      transformedEventEnvelope.sequenceNr,
                      transformedEventEnvelope.eventOption.getOrElse(FilteredPayload),
                      isSnapshotEvent = fromSnapshot(transformedEventEnvelope),
                      transformedEventEnvelope.eventMetadata,
                      transformedEventEnvelope.tags,
                      _))(destination.eventProducerPushDestination.settings.journalWriteTimeout, system.scheduler)
                    .map(_ =>
                      // ack using the original pid in case it was transformed
                      ConsumeEventOut(ConsumeEventOut.Message.Ack(
                        ConsumerEventAck(originalEnvelope.persistenceId, originalEnvelope.sequenceNr))))(
                      ExecutionContexts.parasitic)
                    .recover {
                      case ex: Throwable =>
                        logger.warn(s"Failing event stream because of event writer error", ex)
                        throw ex
                    }(system.executionContext)
                }
                .prepend(Source.future(startEvent.future))

            case None =>
              logger.debug2(
                "Event producer [{}] wanted to push events for stream id [{}] but that is not among the accepted stream ids",
                init.originId,
                init.streamId)
              throw new GrpcServiceException(
                Status.PERMISSION_DENIED.withDescription(
                  s"Events for stream id [${init.streamId}] not accepted by this consumer"))
          }

        case (_, _) =>
          throw new GrpcServiceException(
            Status.INVALID_ARGUMENT.withDescription(
              "Consumer stream in must start with Init event followed by events but got something else"))
      }
  }
}
