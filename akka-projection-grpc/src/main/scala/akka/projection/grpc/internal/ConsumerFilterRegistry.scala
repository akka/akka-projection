/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.projection.grpc.consumer.ConsumerFilter

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
  import ConsumerFilter._
  import ConsumerFilterRegistry._

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
