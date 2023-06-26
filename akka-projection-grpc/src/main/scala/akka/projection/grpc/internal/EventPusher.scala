/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.scaladsl.Handler
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

/**
 * INTERNAL API
 */
private[akka] object EventPusher {
  private val FutureDone = Future.successful(Done)
}

/**
 * INTERNAL API
 *
 * gRPC push protocol handler for the producing side
 */
private[akka] class EventPusher[Event](client: EventConsumerServiceClient, transformation: Transformation)(
    implicit system: ActorSystem[_])
    extends Handler[EventEnvelope[Event]] {
  import akka.projection.grpc.internal.ProtobufProtocolConversions.transformAndEncodeEvent

  private val logger = LoggerFactory.getLogger(classOf[EventPusher[_]])
  private implicit val ec: ExecutionContext = system.executionContext
  private val protoAnySerialization = new ProtoAnySerialization(system)

  override def process(envelope: EventEnvelope[Event]): Future[Done] = {
    transformAndEncodeEvent(transformation, envelope, protoAnySerialization).flatMap {
      case None => EventPusher.FutureDone
      case Some(event) =>
        logger.debug("Pushing event for pid [{}], seq nr [{}]", envelope.persistenceId, envelope.sequenceNr)
        client.consumeEvent(event).map(_ => Done)(ExecutionContexts.parasitic).recover {
          case NonFatal(ex) =>
            throw new RuntimeException(
              s"Error pushing event with pid [${envelope.persistenceId}] and sequence nr [${envelope.sequenceNr}] to consumer",
              ex)
        }
    }
  }

}
