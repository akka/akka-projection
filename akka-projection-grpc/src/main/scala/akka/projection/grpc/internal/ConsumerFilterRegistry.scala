/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.concurrent.duration._
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.UUID

import scala.collection.immutable
import scala.util.Failure
import scala.util.Success

import akka.actor.InvalidActorNameException
import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.annotation.InternalApi
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.ConsumerFilterSettings
import akka.util.Timeout

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConsumerFilterRegistry {
  import ConsumerFilter._

  sealed trait InternalCommand extends Command

  final case class FilterUpdated(streamId: String, criteria: immutable.Seq[FilterCriteria]) extends InternalCommand

  private final case class SubscriberTerminated(subscriber: Subscriber) extends InternalCommand

  private final case class Subscriber(streamId: String, ref: ActorRef[SubscriberCommand])

  def apply(settings: ConsumerFilterSettings): Behavior[Command] = {
    Behaviors.setup { context =>
      new ConsumerFilterRegistry(context, settings).behavior(Map.empty, Map.empty)
    }

  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class ConsumerFilterRegistry(
    context: ActorContext[ConsumerFilter.Command],
    settings: ConsumerFilterSettings) {
  import ConsumerFilter._
  import ConsumerFilterRegistry._

  private def behavior(
      subscribers: Map[Subscriber, immutable.Seq[FilterCriteria]],
      stores: Map[String, ActorRef[ConsumerFilterStore.Command]]): Behavior[Command] = {

    def getOrCreateStore(streamId: String): ActorRef[ConsumerFilterStore.Command] = {
      stores.get(streamId) match {
        case Some(store) => store
        case None =>
          val encodedStreamId = URLEncoder.encode(streamId, StandardCharsets.UTF_8.name)
          try {
            context.spawn(ConsumerFilterStore(context.system, settings, streamId, context.self), encodedStreamId)
          } catch {
            case _: InvalidActorNameException =>
              // There could be a race condition from SubscriberTerminated where the child is stopped,
              // but not removed yet. The actor name isn't important, but could be useful for debugging.
              context.spawn(
                ConsumerFilterStore(context.system, settings, streamId, context.self),
                s"$encodedStreamId-${UUID.randomUUID()}")
          }
      }
    }

    def publishUpdatedFilterToSubscribers(
        streamId: String,
        filter: immutable.Seq[FilterCriteria]): Map[Subscriber, immutable.Seq[FilterCriteria]] = {
      subscribers.map {
        case (sub, subFilter) =>
          if (sub.streamId == streamId) {
            val diff = ConsumerFilter.createDiff(subFilter, filter)
            if (diff.nonEmpty)
              sub.ref ! UpdateFilter(streamId, diff)
            sub -> filter
          } else {
            sub -> subFilter
          }
      }
    }

    def publishToSubscribers(cmd: SubscriberCommand): Unit = {
      context.log.debug("Publish command [{}] to subscribers", cmd)
      subscribers.foreach {
        case (sub, _) =>
          if (sub.streamId == cmd.streamId)
            sub.ref ! cmd
      }
    }

    Behaviors
      .receiveMessage {
        case Subscribe(streamId, initCriteria, subscriberRef) =>
          val subscriber = Subscriber(streamId, subscriberRef)
          context.watchWith(subscriberRef, SubscriberTerminated(subscriber))
          // eagerly start the store
          val store = getOrCreateStore(streamId)

          // trigger a refresh from current filter so that updates between the initCriteria and the Subscribe is flushed
          // to the new subscriber
          implicit val askTimeout: Timeout = 10.seconds
          context
            .ask[ConsumerFilterStore.GetFilter, ConsumerFilter.CurrentFilter](store, ConsumerFilterStore.GetFilter(_)) {
              case Success(ConsumerFilter.CurrentFilter(_, criteria)) => FilterUpdated(streamId, criteria)
              case Failure(_) =>
                context.log
                  .debug("Ask of current filter failed when subscriber for streamId [{}] registered.", streamId)
                null // ignore, it will be ok on next update anyway
            }

          require(!ConsumerFilter.hasRemoveCriteria(initCriteria), "Unexpected RemoveCriteria in a initCriteria")
          behavior(subscribers.updated(subscriber, initCriteria), stores.updated(streamId, store))

        case cmd: UpdateFilter =>
          val store = getOrCreateStore(cmd.streamId)
          store ! ConsumerFilterStore.UpdateFilter(cmd.criteria)

          behavior(subscribers, stores.updated(cmd.streamId, store))

        case GetFilter(streamId, replyTo) =>
          val store = getOrCreateStore(streamId)
          store ! ConsumerFilterStore.GetFilter(replyTo)

          behavior(subscribers, stores.updated(streamId, store))

        case cmd: ReplayWithFilter =>
          publishToSubscribers(cmd)
          Behaviors.same

        case cmd: Replay =>
          publishToSubscribers(cmd)
          Behaviors.same

        case internalCommand: InternalCommand =>
          // extra match for compiler exhaustiveness check
          internalCommand match {
            case FilterUpdated(streamId, criteria) =>
              val newSubscribers = publishUpdatedFilterToSubscribers(streamId, criteria)
              behavior(newSubscribers, stores)

            case SubscriberTerminated(subscriber) =>
              val streamId = subscriber.streamId
              val newSubscribers = subscribers - subscriber
              val newStores =
                if (stores.contains(streamId) && !newSubscribers.exists { case (sub, _) => sub.streamId == streamId }) {
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
