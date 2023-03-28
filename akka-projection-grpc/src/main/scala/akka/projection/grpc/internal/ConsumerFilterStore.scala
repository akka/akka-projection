/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.util.ConcurrentModificationException
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.SupervisorStrategy
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConsumerFilterStore {
  sealed trait Command

  final case class UpdateFilter(criteria: immutable.Seq[FilterCriteria]) extends Command

  final case class GetFilter(replyTo: ActorRef[ConsumerFilter.CurrentFilter]) extends Command

  private def useDistributedData(system: ActorSystem[_]): Boolean = {
    system.classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .provider
      .getClass
      .getName == "akka.cluster.ClusterActorRefProvider" &&
    system.dynamicAccess.getClassFor[Any]("akka.cluster.ddata.typed.scaladsl.DistributedData").isSuccess
  }

  def apply(
      system: ActorSystem[_],
      streamId: String,
      notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]): Behavior[Command] = {
    // ddata dependency is optional
    if (useDistributedData(system)) {
      Behaviors
        .supervise[Command] {
          Behaviors.setup { context =>
            // FIXME DdataConsumerFilterStore have to be created with dynamicAccess?
            new LocalConsumerFilterStore(context, streamId, notifyUpdatesTo).behavior()
          }
        }
        .onFailure(SupervisorStrategy.restart)
    } else {
      Behaviors
        .supervise[Command] {
          Behaviors.setup { context =>
            new LocalConsumerFilterStore(context, streamId, notifyUpdatesTo).behavior()
          }
        }
        .onFailure(SupervisorStrategy.restart)
    }
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object LocalConsumerFilterStore {
  private object StoreExt extends ExtensionId[StoreExt] {
    override def createExtension(system: ActorSystem[_]): StoreExt = new StoreExt
  }

  private class StoreExt extends Extension {
    val filtersByStreamId = new ConcurrentHashMap[String, immutable.Seq[FilterCriteria]]
  }

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class LocalConsumerFilterStore(
    context: ActorContext[ConsumerFilterStore.Command],
    streamId: String,
    notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]) {
  import ConsumerFilterStore._

  // The state must survive the actor lifecycle so keeping the state in an Extension. Single writer per streamId.
  private val storeExt = LocalConsumerFilterStore.StoreExt(context.system)

  def getState(): immutable.Seq[FilterCriteria] = {
    storeExt.filtersByStreamId.computeIfAbsent(streamId, _ => Vector.empty[FilterCriteria])
  }

  def setState(old: immutable.Seq[FilterCriteria], filterCriteria: immutable.Seq[FilterCriteria]): Unit = {
    if (!storeExt.filtersByStreamId.replace(streamId, old, filterCriteria))
      throw new ConcurrentModificationException(s"Unexpected concurrent update of streamId [$streamId]")
    // FIXME this might be too much logging when many ids
    context.log.debug2("Updated filter for streamId [{}] to [{}]", streamId, filterCriteria)
  }

  def behavior(): Behavior[Command] = {
    Behaviors.receiveMessage {
      case UpdateFilter(updatedCriteria) =>
        val oldFilterCriteria = getState()
        val newFilterCriteria = ConsumerFilter.mergeFilter(oldFilterCriteria, updatedCriteria)
        setState(oldFilterCriteria, newFilterCriteria)

        notifyUpdatesTo ! ConsumerFilterRegistry.FilterUpdated(streamId, newFilterCriteria)

        Behaviors.same

      case GetFilter(replyTo) =>
        replyTo ! ConsumerFilter.CurrentFilter(streamId, getState())
        Behaviors.same
    }
  }
}

/**
 * INTERNAL API
 */
// FIXME
//@InternalApi private[akka] class DdataConsumerFilterStore(
//    context: ActorContext[ConsumerFilterStore.Command],
//    streamId: String,
//    notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]) {
//  import ConsumerFilterStore._
//
//  // FIXME
//
//  def behavior(): Behavior[Command] = ???
//}
