/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern._
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.dispatch.ExecutionContexts
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.persistence.FilteredPayload
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PublishedEvent
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.EventWriter
import akka.persistence.typed.internal.EventWriterExtension
import akka.persistence.typed.internal.PublishedEventImpl
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.ReplicatedPublishedEventMetaData
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.internal.proto.ConsumeEventIn
import akka.projection.grpc.internal.proto.ConsumeEventOut
import akka.projection.grpc.internal.proto.ConsumerEventAck
import akka.projection.grpc.internal.proto.ConsumerEventStart
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApi
import akka.projection.grpc.internal.proto.ReplicaInfo
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.stream.scaladsl.Source
import akka.util.Timeout
import io.grpc.Status
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.Promise

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventPusherConsumerServiceImpl {

  private val log = LoggerFactory.getLogger(getClass)

  // See akka.persistence.r2dbc.internal.EnvelopeOrigin, but we don't have a dependency
  // to akka-persistence-r2dbc here
  def fromSnapshot(env: EventEnvelope[_]): Boolean =
    env.source == "SN"

  private case class Destination(
      eventProducerPushDestination: EventProducerPushDestination,
      sendEvent: (EventEnvelope[_], Boolean) => Future[Any],
      replicationSettings: Option[ReplicationSettings[_]])

  /**
   * Projection producer push factory, uses the event writer to pass the events
   */
  def apply(eventProducerDestinations: Set[EventProducerPushDestination], preferProtobuf: ProtoAnySerialization.Prefer)(
      implicit system: ActorSystem[_]): EventPusherConsumerServiceImpl = {
    val eventWriterExtension = EventWriterExtension(system)
    import system.executionContext
    lazy val sharding = ClusterSharding(system)

    val destinationPerStreamId = eventProducerDestinations.map { d =>
      val writerForJournal = eventWriterExtension.writerForJournal(d.journalPluginId)

      val sendEvent = d.replicationSettings match {
        case Some(replicationSettings) =>
          implicit val timeout: Timeout = replicationSettings.entityEventReplicationTimeout

          { (envelope: EventEnvelope[_], fillSequenceNumberGaps: Boolean) =>
            if (envelope.filtered) {
              log.trace("Ignoring filtered event [{}] for pid [{}]", envelope.sequenceNr, envelope.persistenceId)
              Future.successful(Done)
            } else {
              envelope.eventMetadata match {
                case Some(replicatedEventMetadata: ReplicatedEventMetadata) =>
                  // send event to entity in this replica
                  val replicationId = ReplicationId.fromString(envelope.persistenceId)
                  val destinationReplicaId = replicationId.withReplica(replicationSettings.selfReplicaId)
                  if (fillSequenceNumberGaps)
                    throw new IllegalArgumentException(
                      s"fillSequenceNumberGaps can not be true for RES (pid ${envelope.persistenceId}, seq nr ${envelope.sequenceNr} from ${replicationId}")
                  val entityRef = sharding
                    .entityRefFor(replicationSettings.entityTypeKey, destinationReplicaId.entityId)
                    .asInstanceOf[EntityRef[PublishedEvent]]
                  val ask = { () =>
                    log.trace(
                      "Passing event [{}] for pid [{}] to replicated entity",
                      envelope.sequenceNr,
                      envelope.persistenceId)
                    entityRef.ask[Done](
                      replyTo =>
                        PublishedEventImpl(
                          replicationId.persistenceId,
                          replicatedEventMetadata.originSequenceNr,
                          envelope.event,
                          envelope.timestamp,
                          Some(
                            new ReplicatedPublishedEventMetaData(
                              replicatedEventMetadata.originReplica,
                              replicatedEventMetadata.version)),
                          Some(replyTo)))
                  }

                  // try a few times before tearing stream down, forcing the client to restart/reconnect
                  val askResult = akka.pattern.retry(
                    ask,
                    replicationSettings.edgeReplicationDeliveryRetries,
                    replicationSettings.edgeReplicationDeliveryMinBackoff,
                    replicationSettings.edgeReplicationDeliveryMaxBackoff,
                    0.2d)(system.executionContext, system.classicSystem.scheduler)

                  askResult.failed.foreach(
                    error =>
                      log.warn(
                        s"Failing replication stream from [${replicatedEventMetadata.originReplica.id}], event pid [${envelope.persistenceId}], seq_nr [${envelope.sequenceNr}]",
                        error))
                  askResult

                case unexpected =>
                  throw new IllegalArgumentException(
                    s"Got unexpected type of event envelope metadata: ${unexpected.getClass} (pid [${envelope.persistenceId}], seq_nr [${envelope.sequenceNr}]" +
                    ", is the remote entity really a Replicated Event Sourced Entity?")
              }
            }
          }

        case None => { (envelope: EventEnvelope[_], fillSequenceNumberGaps: Boolean) =>
          log.trace("Passing event [{}] for pid [{}] to event writer", envelope.sequenceNr, envelope.persistenceId)
          writerForJournal.askWithStatus[EventWriter.WriteAck](
            replyTo =>
              EventWriter.Write(
                persistenceId = envelope.persistenceId,
                sequenceNumber = envelope.sequenceNr,
                event = envelope.eventOption.getOrElse(FilteredPayload),
                isSnapshotEvent = fromSnapshot(envelope),
                fillSequenceNumberGaps = fillSequenceNumberGaps,
                metadata = envelope.eventMetadata,
                tags = envelope.tags,
                replyTo = replyTo))(d.settings.journalWriteTimeout, system.scheduler)
        }
      }

      d.acceptedStreamId -> Destination(d, sendEvent, d.replicationSettings)
    }.toMap

    new EventPusherConsumerServiceImpl(eventProducerDestinations, preferProtobuf, destinationPerStreamId)
  }
}

/**
 * INTERNAL API
 *
 * gRPC push protocol service for the consuming side
 */
@InternalApi
private[akka] final class EventPusherConsumerServiceImpl private (
    eventProducerDestinations: Set[EventProducerPushDestination],
    preferProtobuf: ProtoAnySerialization.Prefer,
    destinationPerStreamId: Map[String, EventPusherConsumerServiceImpl.Destination])(implicit system: ActorSystem[_])
    extends EventConsumerServicePowerApi {

  import ProtobufProtocolConversions._

  private val logger = LoggerFactory.getLogger(classOf[EventPusherConsumerServiceImpl])

  private val protoAnySerialization =
    new ProtoAnySerialization(system, eventProducerDestinations.flatMap(_.protobufDescriptors).toVector, preferProtobuf)
  private val perPartitionParallelism =
    system.settings.config.getInt("akka.persistence.typed.event-writer.max-batch-size") / 2

  private implicit val ec: ExecutionContext = system.executionContext

  logger.info(
    "Passive event consumer service created, accepting stream ids [{}]",
    destinationPerStreamId
      .map { case (id, dest) => id + dest.replicationSettings.map(_ => s" (replicated entity)").getOrElse("") }
      .mkString(", "))

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
                ConsumeEventOut(ConsumeEventOut.Message.Start(ConsumerEventStart(
                  toProtoFilterCriteria(destination.eventProducerPushDestination.filters),
                  destination.replicationSettings
                    .map(rs => ReplicaInfo(rs.selfReplicaId.id, rs.otherReplicas.map(_.replicaId.id).toSeq))))))

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

                  destination
                    .sendEvent(transformedEventEnvelope, init.fillSequenceNumberGaps)
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
