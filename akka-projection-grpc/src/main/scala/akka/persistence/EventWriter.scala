/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.persistence

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.annotation.InternalStableApi
import akka.pattern.StatusReply
import akka.persistence.journal.Tagged

import java.net.URLEncoder
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// FIXME move to akka-persistence-typed for access

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object EventWriterExtension extends ExtensionId[EventWriterExtension] {
  def createExtension(system: ActorSystem[_]): EventWriterExtension = new EventWriterExtension(system)

  def get(system: ActorSystem[_]): EventWriterExtension = apply(system)
}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] class EventWriterExtension(system: ActorSystem[_]) extends Extension {
  private val writersPerJournalId = new ConcurrentHashMap[String, ActorRef[EventWriter.Command]]()

  def writerForJournal(journalId: Option[String]): ActorRef[EventWriter.Command] =
    writersPerJournalId.computeIfAbsent(
      journalId.getOrElse(""), { _ =>
        system.systemActorOf(
          EventWriter(journalId.getOrElse("")),
          s"EventWriter-${URLEncoder.encode(journalId.getOrElse("default"), "UTF-8")}")
      })

}

/**
 * INTERNAL API
 */
@InternalStableApi
private[akka] object EventWriter {

  private val instanceCounter = new AtomicInteger(1)

  sealed trait Command
  final case class Write(
      persistenceId: String,
      sequenceNumber: Long,
      event: Any,
      metadata: Option[Any],
      tags: Set[String],
      replyTo: ActorRef[StatusReply[WriteAck]])
      extends Command
  final case class WriteAck(persistenceId: String, sequenceNumber: Long)

  private def emptyWaitingForWrite = Vector.empty[(PersistentRepr, ActorRef[StatusReply[WriteAck]])]
  private case class StateForPid(
      waitingForReply: Map[Long, ActorRef[StatusReply[WriteAck]]],
      waitingForWrite: Vector[(PersistentRepr, ActorRef[StatusReply[WriteAck]])] = emptyWaitingForWrite)

  def apply(journalPluginId: String): Behavior[Command] =
    Behaviors
      .supervise(Behaviors
        .setup[AnyRef] { context =>
          val maxBatchSize = 20 // FIXME from config
          val actorInstanceId = instanceCounter.getAndIncrement()
          val writerUuid = UUID.randomUUID().toString
          val journal = Persistence(context.system).journalFor(journalPluginId)
          context.log.debug("Event writer for journal [{}] starting up", journalPluginId)

          var perPidWriteState = Map.empty[String, StateForPid]

          def handleUpdatedStateForPid(pid: String, newStateForPid: StateForPid): Unit = {
            if (newStateForPid.waitingForReply.nonEmpty) {
              // more waiting replyTo before we could batch it or scrap the entry
              perPidWriteState = perPidWriteState.updated(pid, newStateForPid)
            } else {
              if (newStateForPid.waitingForWrite.isEmpty) {
                perPidWriteState = perPidWriteState - pid
              } else {
                // batch waiting for pid
                if (context.log.isTraceEnabled())
                  context.log.traceN(
                    "Writing batch of {} events for pid [{}], seq nrs [{}-{}]",
                    newStateForPid.waitingForWrite.size,
                    pid,
                    newStateForPid.waitingForWrite.head._1.sequenceNr,
                    newStateForPid.waitingForWrite.last._1.sequenceNr)
                val batchWrite = AtomicWrite(newStateForPid.waitingForWrite.map { case (repr, _) => repr })
                journal ! JournalProtocol
                  .WriteMessages(batchWrite :: Nil, context.self.toClassic, actorInstanceId)

                val newReplyTo = newStateForPid.waitingForWrite.map {
                  case (repr, replyTo) => repr.sequenceNr -> replyTo
                }.toMap
                perPidWriteState = perPidWriteState.updated(pid, StateForPid(newReplyTo, emptyWaitingForWrite))
              }
            }
          }

          def handleJournalResponse(response: JournalProtocol.Response): Behavior[AnyRef] =
            response match {
              case JournalProtocol.WriteMessageSuccess(message, `actorInstanceId`) =>
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

              case JournalProtocol.WriteMessageFailure(message, error, `actorInstanceId`) =>
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

          Behaviors.receiveMessage {
            case Write(persistenceId, sequenceNumber, event, metadata, tags, replyTo) =>
              val payload = if (tags.isEmpty) event else Tagged(event, tags)
              val repr = PersistentRepr(
                payload,
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
                    if (context.log.isTraceEnabled)
                      context.log.traceN(
                        "Writing event persistence id [{}], sequence nr [{}], payload {}",
                        persistenceId,
                        sequenceNumber,
                        event)
                    val write = AtomicWrite(reprWithMeta) :: Nil
                    journal ! JournalProtocol.WriteMessages(write, context.self.toClassic, actorInstanceId)
                    StateForPid(Map(reprWithMeta.sequenceNr -> replyTo), emptyWaitingForWrite)
                  case Some(state) =>
                    // write in progress for pid, add write to batch and perform once current write completes
                    if (state.waitingForWrite.size == maxBatchSize) {
                      replyTo ! StatusReply.error(
                        s"Max batch reached for pid $persistenceId, at most $maxBatchSize writes for " +
                        "the same pid may be in flight at the same time")
                      state
                    } else {
                      if (context.log.isTraceEnabled)
                        context.log.traceN(
                          "Writing event in progress for persistence id [{}], adding sequence nr [{}], payload {} to batch",
                          persistenceId,
                          sequenceNumber,
                          event)
                      state.copy(waitingForWrite = state.waitingForWrite :+ ((reprWithMeta, replyTo)))
                    }
                }
              perPidWriteState = perPidWriteState.updated(persistenceId, newStateForPid)
              Behaviors.same

            case response: JournalProtocol.Response => handleJournalResponse(response)

            case unexpected =>
              context.log.warn("Unexpected message sent to EventWriter [{}], ignored", unexpected.getClass)
              Behaviors.same

          }
        })
      .onFailure[Exception](SupervisorStrategy.restart)
      .narrow[Command]

}
