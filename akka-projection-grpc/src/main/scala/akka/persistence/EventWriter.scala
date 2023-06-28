/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalApi
import akka.pattern.StatusReply

import java.util.UUID

// FIXME move to akka-persistence-typed for access

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object EventWriter {

  sealed trait Command
  final case class Write(
      persistenceId: String,
      sequenceNumber: Long,
      event: Any,
      metadata: Option[Any],
      replyTo: ActorRef[StatusReply[WriteAck]])
      extends Command
  final case class WriteAck(persistenceId: String, sequenceNumber: Long)

  private case class StateForPid(
      waitingForReply: Map[Long, ActorRef[StatusReply[WriteAck]]],
      waitingForWriteReverse: List[(PersistentRepr, ActorRef[StatusReply[WriteAck]])])

  def apply(journalPluginId: String): Behavior[Command] =
    Behaviors
      .supervise(Behaviors
        .setup[AnyRef] { context =>
          val writerUuid = UUID.randomUUID().toString
          val journal = Persistence(context.system).journalFor(journalPluginId)
          context.log.debug("Event writer for journal [{}] starting up", journalPluginId)

          var perPidWriteState = Map.empty[String, StateForPid]

          def handleUpdatedStateForPid(pid: String, newStateForPid: StateForPid): Unit = {
            if (newStateForPid.waitingForReply.nonEmpty) {
              // more waiting replyTo before we could batch it or scrap the entry
              perPidWriteState = perPidWriteState.updated(pid, newStateForPid)
            } else {
              if (newStateForPid.waitingForWriteReverse.isEmpty) {
                perPidWriteState = perPidWriteState - pid
              } else {
                // batch waiting for pid
                val inOrder = newStateForPid.waitingForWriteReverse.reverse
                val batchWrite = AtomicWrite(inOrder.map { case (repr, _) => repr })
                journal ! JournalProtocol
                  .WriteMessages(batchWrite :: Nil, context.self.toClassic, context.self.path.uid)

                val newReplyTo = inOrder.map { case (repr, replyTo) => repr.sequenceNr -> replyTo }.toMap
                // FIXME do we need to limit batch size?
                perPidWriteState = perPidWriteState.updated(pid, StateForPid(newReplyTo, Nil))
              }
            }
          }

          Behaviors.receiveMessage {
            case Write(persistenceId, sequenceNumber, event, metadata, replyTo) =>
              if (context.log.isTraceEnabled)
                context.log.traceN(
                  "Writing event persistence id [{}], sequence nr [{}], payload {}",
                  persistenceId,
                  sequenceNumber,
                  event)
              val repr = PersistentRepr(
                event,
                persistenceId = persistenceId,
                sequenceNr = sequenceNumber,
                manifest = "", // adapters would be on the producing side, already applied
                writerUuid = writerUuid,
                sender = akka.actor.ActorRef.noSender)

              val reprWithMeta = metadata match {
                case Some(meta) => repr.withMetadata(meta)
                case _          => repr
              }

              val newStateForPid =
                perPidWriteState.get(persistenceId) match {
                  case None =>
                    val write = AtomicWrite(reprWithMeta) :: Nil
                    journal ! JournalProtocol.WriteMessages(write, context.self.toClassic, context.self.path.uid)
                    StateForPid(Map(reprWithMeta.sequenceNr -> replyTo), Nil)
                  case Some(state) =>
                    // write in progress for pid, add write to batch and perform once current write completes
                    state.copy(waitingForWriteReverse = (reprWithMeta, replyTo) :: state.waitingForWriteReverse)
                }
              perPidWriteState = perPidWriteState.updated(persistenceId, newStateForPid)
              Behaviors.same

            case JournalProtocol.WriteMessageSuccess(message, _) =>
              val pid = message.persistenceId
              val sequenceNr = message.sequenceNr
              perPidWriteState.get(pid) match {
                case None =>
                  throw new IllegalStateException(
                    s"Got write success reply for event with no waiting request, probably a bug (pid $pid, seq nr $sequenceNr)")
                case Some(stateForPid) =>
                  stateForPid.waitingForReply.get(sequenceNr) match {
                    case None =>
                      throw new IllegalStateException(
                        s"Got write success reply for event with no waiting request, probably a bug (pid $pid, seq nr $sequenceNr)")
                    case Some(waiting) =>
                      if (context.log.isTraceEnabled)
                        context.log.trace2(
                          "Successfully wrote event persistence id [{}], sequence nr [{}]",
                          pid,
                          message.sequenceNr)
                      waiting ! StatusReply.success(WriteAck(pid, sequenceNr))
                      val newState = stateForPid.copy(waitingForReply = stateForPid.waitingForReply - sequenceNr)
                      handleUpdatedStateForPid(pid, newState)
                      Behaviors.same
                  }
              }

            case JournalProtocol.WriteMessageFailure(message, error, _) =>
              val pid = message.persistenceId
              val sequenceNr = message.sequenceNr
              perPidWriteState.get(pid) match {
                case None =>
                  throw new IllegalStateException(
                    s"Got error reply for event with no waiting request, probably a bug (pid $pid, seq nr $sequenceNr)",
                    error)
                case Some(state) =>
                  state.waitingForReply.get(sequenceNr) match {
                    case None =>
                      throw new IllegalStateException(
                        s"Got error reply for event with no waiting request, probably a bug (pid $pid, seq nr $sequenceNr)",
                        error)
                    case Some(replyTo) =>
                      context.log.warnN(
                        "Failed writing event persistence id [{}], sequence nr [{}]: {}",
                        pid,
                        sequenceNr,
                        error.getMessage)
                      replyTo ! StatusReply.error(error.getMessage)
                      val newState = state.copy(waitingForReply = state.waitingForReply - sequenceNr)
                      handleUpdatedStateForPid(pid, newState)
                      Behaviors.same
                  }
              }

            case _ =>
              // ignore all other journal protocol messages
              Behaviors.same

          }
        })
      .onFailure[Exception](SupervisorStrategy.restart)
      .narrow[Command]

}
