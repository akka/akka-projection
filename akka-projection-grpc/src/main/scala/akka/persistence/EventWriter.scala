/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalApi
import akka.pattern.StatusReply
import akka.persistence.query.typed.EventEnvelope

import java.util
import java.util.UUID

// FIXME move to akka-persistence-typed for access

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventWriter {

  sealed trait Command
  final case class Write(envelope: EventEnvelope[Any], replyTo: ActorRef[StatusReply[Done]]) extends Command

  def apply(journalPluginId: String): Behavior[Command] =
    Behaviors
      .setup[AnyRef] { context =>
        val writerUuid = UUID.randomUUID().toString
        val journal = Persistence(context.system).journalFor(journalPluginId)
        context.log.debug("Event writer for journal [{}] starting up", journalPluginId)

        val waitingForResponse = new util.HashMap[(String, Long), ActorRef[StatusReply[Done]]]()

        Behaviors.receiveMessage {
          case Write(envelope, replyTo) =>
            // FIXME trace or remove
            context.log.debug(
              "Writing event persistence id [{}], sequence nr [{}]",
              envelope.persistenceId,
              envelope.sequenceNr)
            val repr = PersistentRepr(
              envelope.event,
              persistenceId = envelope.persistenceId,
              sequenceNr = envelope.sequenceNr,
              manifest = "", // adapters would be on the producing side, already applied
              writerUuid = writerUuid,
              sender = akka.actor.ActorRef.noSender)

            val write = AtomicWrite(envelope.eventMetadata match {
                case Some(meta) => repr.withMetadata(meta)
                case _          => repr
              }) :: Nil

            waitingForResponse.put((envelope.persistenceId, envelope.sequenceNr), replyTo)

            journal ! JournalProtocol.WriteMessages(
              write,
              // FIXME is this the ref to pass?
              context.self.toClassic,
              context.self.path.uid)
            Behaviors.same

          case JournalProtocol.WriteMessageSuccess(message, _) =>
            val pidSeqnr = (message.persistenceId, message.sequenceNr)
            waitingForResponse.get(pidSeqnr) match {
              case null =>
                context.log.warn2(
                  "Got write success reply for event with no waiting request, probably a bug (pid {}, seq nr {})",
                  message.persistenceId,
                  message.sequenceNr)
                Behaviors.same
              case replyTo =>
                // FIXME trace or remove
                context.log.debug2(
                  "Successfully wrote event persistence id [{}], sequence nr [{}]",
                  message.persistenceId,
                  message.sequenceNr)
                replyTo ! StatusReply.success(Done)
                waitingForResponse.remove(pidSeqnr)
                Behaviors.same
            }
            Behaviors.same

          case JournalProtocol.WriteMessageFailure(message, error, _) =>
            val pidSeqnr = (message.persistenceId, message.sequenceNr)
            waitingForResponse.get(pidSeqnr) match {
              case null =>
                context.log.warn2(
                  "Got error reply for event with no waiting request, probably a bug (pid {}, seq nr {})",
                  message.persistenceId,
                  message.sequenceNr)
                Behaviors.same
              case replyTo =>
                context.log.warnN(
                  "Failed writing event persistence id [{}], sequence nr [{}]: {}",
                  message.persistenceId,
                  message.sequenceNr,
                  error.getMessage)

                replyTo ! StatusReply.error(error.getMessage)

                waitingForResponse.remove(pidSeqnr)
                Behaviors.same
            }

          case _ =>
            // ignore all other journal protocol messages
            Behaviors.same

        }
      }
      .narrow[Command]

  val WriteMessages = JournalProtocol.WriteMessages.apply _

}
