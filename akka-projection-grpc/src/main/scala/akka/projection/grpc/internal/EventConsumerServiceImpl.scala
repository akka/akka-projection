/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventConsumerService
import com.google.protobuf.empty.Empty
import org.slf4j.LoggerFactory

import scala.concurrent.Future

/**
 * INTERNAL API
 */
private[akka] final class EventConsumerServiceImpl(consumeEvent: EventEnvelope[Any] => Future[Done])(
    implicit system: ActorSystem[_])
    extends EventConsumerService {
  private val logger = LoggerFactory.getLogger(classOf[EventProducerServiceImpl])

  private val protoAnySerialization = new ProtoAnySerialization(system)
  override def consumeEvent(in: Event): Future[Empty] = {
    val envelope = ProtobufProtocolConversions.eventToEnvelope[Any](in, protoAnySerialization)
    logger.debug("Saw event [{}] for pid [{}]", envelope.sequenceNr, envelope.persistenceId)

    consumeEvent(envelope).map(_ => Empty.defaultInstance)(ExecutionContexts.parasitic)
  }

}
