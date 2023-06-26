/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.scaladsl.AskPattern.schedulerFromActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.EventWriter
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.internal.proto.Event
import akka.projection.grpc.internal.proto.EventConsumerService
import akka.util.ByteString
import akka.util.Timeout
import com.google.protobuf.empty.Empty
import org.slf4j.LoggerFactory

import java.net.URLEncoder
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * INTERNAL API
 *
 * gRPC push protocol service for the consuming side
 */
private[akka] object EventConsumerServiceImpl {

  def directJournalConsumer(journalPluginId: String)(implicit system: ActorSystem[_]): EventConsumerServiceImpl = {
    val eventWriter = system.systemActorOf(
      EventWriter(journalPluginId),
      s"EventWriter-${URLEncoder.encode(journalPluginId, ByteString.UTF_8)}")
    implicit val timeout: Timeout = 5.seconds // FIXME from config or can we get rid of it

    val writeEventToJournal: EventEnvelope[Any] => Future[Done] = envelope =>
      eventWriter.askWithStatus[Done](replyTo => EventWriter.Write(envelope, replyTo))

    new EventConsumerServiceImpl(writeEventToJournal)
  }

}

/**
 * INTERNAL API
 */
private[akka] final class EventConsumerServiceImpl(consumeEvent: EventEnvelope[Any] => Future[Done])(
    implicit system: ActorSystem[_])
    extends EventConsumerService {
  private val logger = LoggerFactory.getLogger(classOf[EventConsumerServiceImpl])

  private val protoAnySerialization = new ProtoAnySerialization(system)
  override def consumeEvent(in: Event): Future[Empty] = {
    // FIXME do we need to make sure events for the same pid are ordered? Should be single writer per pid but?
    val envelope = ProtobufProtocolConversions.eventToEnvelope[Any](in, protoAnySerialization)
    logger.debug("Saw event [{}] for pid [{}]", envelope.sequenceNr, envelope.persistenceId)

    consumeEvent(envelope).map(_ => Empty.defaultInstance)(ExecutionContexts.parasitic)
  }

}
