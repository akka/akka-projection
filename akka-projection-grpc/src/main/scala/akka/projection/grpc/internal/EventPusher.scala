/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.ProtobufProtocolConversions.offsetToProtoOffset
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.scaladsl.Handler
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
    implicit system: ActorSystem[_])
    extends Handler[EventEnvelope[Event]] {
  import akka.projection.grpc.internal.ProtobufProtocolConversions.transformAndEncodeEvent

  private val logger = LoggerFactory.getLogger(classOf[EventPusher[_]])
  private implicit val ec: ExecutionContext = system.executionContext
  private val protoAnySerialization = new ProtoAnySerialization(system)

  override def process(envelope: EventEnvelope[Event]): Future[Done] = {
    val encodedEvent: Future[Option[proto.Event]] =
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

    encodedEvent
      .flatMap {
        case Some(protoEvent) =>
          client
            .consumeEvent(protoEvent)
        case None =>
          // Filtered or transformed to None, we still need to push a placeholder to not get seqnr gaps on the receiving side
          client.consumeEvent(proto.Event(
            persistenceId = envelope.persistenceId,
            seqNr = envelope.sequenceNr,
            slice = envelope.slice,
            offset = offsetToProtoOffset(envelope.offset),
            payload = None,
            tags = Seq.empty
            // FIXME do we still need to pass along metadata?
          ))
      }
      .map(_ => Done)(ExecutionContexts.parasitic)
      .recover {
        case NonFatal(ex) =>
          throw new RuntimeException(
            s"Error pushing event with pid [${envelope.persistenceId}] and sequence nr [${envelope.sequenceNr}] to consumer",
            ex)
      }
  }

}
