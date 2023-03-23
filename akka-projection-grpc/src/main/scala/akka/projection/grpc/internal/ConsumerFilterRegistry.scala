/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import scala.collection.immutable

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

  sealed trait InternalCommand extends Command

  final case class FilterUpdated(streamId: String, criteria: immutable.Seq[FilterCriteria]) extends InternalCommand

  private final case class SubscriberTerminated(subscriber: Subscriber) extends InternalCommand

  private final case class Subscriber(streamId: String, ref: ActorRef[SubscriberCommand])

  def apply(): Behavior[Command] = {
    Behaviors.setup { context =>
      new ConsumerFilterRegistry(context).behavior(Set.empty, Map.empty)
    }

  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ConsumerFilterRegistry(context: ActorContext[ConsumerFilter.Command]) {
  import ConsumerFilter._
  import ConsumerFilterRegistry._

  private def behavior(
      subscribers: Set[Subscriber],
      stores: Map[String, ActorRef[ConsumerFilterStore.Command]]): Behavior[Command] = {

    def getOrCreateStore(streamId: String): ActorRef[ConsumerFilterStore.Command] = {
      stores.get(streamId) match {
        case Some(store) => store
        case None =>
          context.spawn(
            ConsumerFilterStore(context.system, streamId, context.self),
            URLEncoder.encode(streamId, StandardCharsets.UTF_8.name))
      }
    }

    def publishToSubscribers(cmd: SubscriberCommand): Unit = {
      context.log.debug("Publish command [{}] to subscribers", cmd)
      subscribers.foreach { sub =>
        if (sub.streamId == cmd.streamId)
          sub.ref ! cmd
      }
    }

    Behaviors
      .receiveMessage {
        case sub: Subscribe =>
          val subscriber = Subscriber(sub.streamId, sub.subscriber)
          context.watchWith(sub.subscriber, SubscriberTerminated(subscriber))
          // eagerly start the store
          val store = getOrCreateStore(sub.streamId)
          behavior(subscribers + subscriber, stores.updated(sub.streamId, store))

        case cmd: UpdateFilter =>
          val store = getOrCreateStore(cmd.streamId)
          store ! ConsumerFilterStore.UpdateFilter(cmd.criteria)

          behavior(subscribers, stores.updated(cmd.streamId, store))

        case GetFilter(streamId, replyTo) =>
          val store = getOrCreateStore(streamId)
          store ! ConsumerFilterStore.GetFilter(replyTo)

          behavior(subscribers, stores.updated(streamId, store))

        case cmd: Replay =>
          publishToSubscribers(cmd)

          Behaviors.same

        case internalCommand: InternalCommand =>
          // extra match for compiler exhaustiveness check
          internalCommand match {
            case FilterUpdated(streamId, criteria) =>
              // FIXME the critiera is the full accumulated critera, we should create a diff per subscriber
              publishToSubscribers(UpdateFilter(streamId, criteria))
              Behaviors.same

            case SubscriberTerminated(subscriber) =>
              val streamId = subscriber.streamId
              val newSubscribers = subscribers - subscriber
              val newStores =
                if (stores.contains(streamId) && !newSubscribers.exists(_.streamId == streamId)) {
                  // no more subscribers of the streamId, we can stop the store
                  context.stop(stores(streamId))
                  stores - streamId
                } else
                  stores
              behavior(newSubscribers, newStores)
          }

        case unknown: Command =>
          // Command is not sealed because in different file
          throw new IllegalArgumentException(s"Unexpected Command [${unknown.getClass.getName}]. This is a bug.")

      }
  }

}
