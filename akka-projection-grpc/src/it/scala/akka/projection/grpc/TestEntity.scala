/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior

object TestEntity {
  sealed trait Command
  final case class Persist(payload: Any) extends Command
  final case class Ping(replyTo: ActorRef[Done]) extends Command
  final case class Stop(replyTo: ActorRef[Done]) extends Command

  def apply(pid: PersistenceId): Behavior[Command] = {
    Behaviors.setup { context =>
      EventSourcedBehavior[Command, Any, String](
        persistenceId = pid,
        "", { (_, command) =>
          command match {
            case command: Persist =>
              context.log.debugN(
                "Persist [{}], pid [{}], seqNr [{}]",
                command.payload,
                pid.id,
                EventSourcedBehavior.lastSequenceNumber(context) + 1)
              Effect.persist(command.payload)
            case Ping(replyTo) =>
              replyTo ! Done
              Effect.none
            case Stop(replyTo) =>
              replyTo ! Done
              Effect.stop()
          }
        },
        (_, _) => "")
    }
  }
}
