/**
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.consumer.scaladsl

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
  final case class Subscribe(streamId: String, subscriber: ActorRef[FilterCommand]) extends Command

  sealed trait FilterCommand extends Command
  final case class FilterEntityIds(streamId: String, include: Set[String], exclude: Set[String]) extends FilterCommand

  // FIXME What kind of filters should we support? Per entity like this FilterEntityIds is very fine grained.
  //       Regular expression matches on the entity ids would be nice, but there we have a challenge that we
  //       can't load previous events when an "include" filter is added. We don't know the matching pids unless
  //       we would keep track of all pids via the PersistenceIdsQuery. Then we would instead have to load
  //       previous events on demand when we receive an "unexpected" seqNr. Or we would just support receiving
  //       events after the filter was added.

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

  private final case class Subscriber(streamId: String, ref: ActorRef[FilterCommand])

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

      case filter: FilterEntityIds =>
        // FIXME the registered filters (aggregated) must also be kept in state and distributed to other nodes in the cluster

        context.log.debug("Publish filter [{}] to subscribers", filter)
        subscribers.foreach { sub =>
          if (sub.streamId == filter.streamId)
            sub.ref ! filter
        }
        Behaviors.same
    }
  }

}
