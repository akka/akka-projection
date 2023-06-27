/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.Done
import akka.actor.typed.ActorRef
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

  def directJournalConsumer(journalPluginId: String, persistenceIdTransformer: String => String)(
      implicit system: ActorSystem[_]): EventConsumerServiceImpl = {
    // FIXME is this name unique, could we create multiple for the same journal? (we wouldn't be able to bind them to the same port)
    val eventWriter = system.systemActorOf(
      EventWriter(journalPluginId),
      s"EventWriter-${URLEncoder.encode(journalPluginId, ByteString.UTF_8)}")

    new EventConsumerServiceImpl(eventWriter, persistenceIdTransformer)
  }

}

/**
 * INTERNAL API
 */
private[akka] final class EventConsumerServiceImpl(
    eventWriter: ActorRef[EventWriter.Command],
    persistenceIdTransformer: String => String)(implicit system: ActorSystem[_])
    extends EventConsumerService {
  private val logger = LoggerFactory.getLogger(classOf[EventConsumerServiceImpl])

  private val protoAnySerialization = new ProtoAnySerialization(system)

  implicit val timeout: Timeout = 5.seconds // FIXME from config or can we get rid of it
  private def writeEventToJournal(envelope: EventEnvelope[Any]): Future[Done] = {
    // FIXME would we want request metadata/producer ip/entire envelope in to persistence id transformer?
    val persistenceId = persistenceIdTransformer(envelope.persistenceId)

    // FIXME can we skip the ask for each event?
    eventWriter.askWithStatus[Done](replyTo =>
      EventWriter.Write(persistenceId, envelope.sequenceNr, envelope.event, envelope.eventMetadata, replyTo))
  }

  override def consumeEvent(in: Event): Future[Empty] = {
    // FIXME do we need to make sure events for the same pid are ordered? Should be single writer per pid but?
    val envelope = ProtobufProtocolConversions.eventToEnvelope[Any](in, protoAnySerialization)
    logger.trace("Saw event [{}] for pid [{}]", envelope.sequenceNr, envelope.persistenceId)

    writeEventToJournal(envelope).map(_ => Empty.defaultInstance)(ExecutionContexts.parasitic)
  }

}
