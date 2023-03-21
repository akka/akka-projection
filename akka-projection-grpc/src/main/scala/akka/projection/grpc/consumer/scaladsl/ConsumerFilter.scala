/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import scala.collection.immutable
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi

object ConsumerFilter extends ExtensionId[ConsumerFilter] {
  sealed trait Command
  sealed trait SubscriberCommand extends Command {
    def streamId: String
  }
  final case class Subscribe(streamId: String, subscriber: ActorRef[SubscriberCommand]) extends Command

  final case class FilterCommand(streamId: String, criteria: immutable.Seq[FilterCriteria]) extends SubscriberCommand

  final case class ReplayCommand(streamId: String, entityOffsets: Set[EntityIdOffset]) extends SubscriberCommand

  sealed trait FilterCriteria
  final case class ExcludeRegexEntityIds(matching: Set[String]) extends FilterCriteria
  final case class ExcludeEntityIds(entityIds: Set[String]) extends FilterCriteria
  final case class IncludeEntityIds(entityOffsets: Set[EntityIdOffset]) extends FilterCriteria

  final case class EntityIdOffset(entityId: String, seqNr: Long)

  override def createExtension(system: ActorSystem[_]): ConsumerFilter = new ConsumerFilter(system)

}

class ConsumerFilter(system: ActorSystem[_]) extends Extension {

  val ref: ActorRef[ConsumerFilter.Command] =
    system.systemActorOf(ConsumerFilterRegistry(), "projectionGrpcConsumerFilter", Props.empty)

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConsumerFilterRegistry {
  import ConsumerFilter._

  private final case class Subscriber(streamId: String, ref: ActorRef[SubscriberCommand])

  def apply(): Behavior[Command] = {
    Behaviors.setup { context =>
      new ConsumerFilterRegistry(context).behavior(Set.empty)
    }

  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ConsumerFilterRegistry(context: ActorContext[ConsumerFilter.Command]) {
  import ConsumerFilterRegistry._
  import ConsumerFilter._

  private def behavior(subscribers: Set[Subscriber]): Behavior[Command] = {
    Behaviors.receiveMessage {
      case sub: Subscribe =>
        // FIXME watch termination of subscriber, cleanup and such

        // FIXME should it be for a specific producer service? In the end that is only identified by the grpc client
        //       settings. It would be possible several GrpcReadJournal for different producers but still with the same
        //       streamId. Is it important to be able to filter those separately?

        behavior(subscribers + Subscriber(sub.streamId, sub.subscriber))

      case cmd: SubscriberCommand =>
        // FIXME the registered filters (aggregated) must also be kept in state and distributed to other nodes in the cluster

        context.log.debug("Publish command [{}] to subscribers", cmd)
        subscribers.foreach { sub =>
          if (sub.streamId == cmd.streamId)
            sub.ref ! cmd
        }
        Behaviors.same
    }
  }

}
