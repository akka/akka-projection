/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.util.ConcurrentModificationException
import java.util.concurrent.ConcurrentHashMap

import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success

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
import akka.cluster.ddata.Key
import akka.cluster.ddata.ReplicatedData
import akka.cluster.ddata.typed.scaladsl.DistributedData
import akka.cluster.ddata.typed.scaladsl.Replicator
import akka.cluster.ddata.typed.scaladsl.ReplicatorMessageAdapter
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.FilterCriteria
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[akka] object ConsumerFilterStore {
  sealed trait Command

  final case class UpdateFilter(criteria: immutable.Seq[FilterCriteria]) extends Command

  final case class GetFilter(replyTo: ActorRef[ConsumerFilter.CurrentFilter]) extends Command

  def apply(
      system: ActorSystem[_],
      streamId: String,
      notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]): Behavior[Command] = {
    // ddata dependency is optional
    if (useDistributedData(system)) {
      createDdataConsumerFilterStore(system, streamId, notifyUpdatesTo)
    } else {
      LocalConsumerFilterStore(streamId, notifyUpdatesTo)
    }
  }

  def useDistributedData(system: ActorSystem[_]): Boolean = {
    system.classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .provider
      .getClass
      .getName == "akka.cluster.ClusterActorRefProvider" &&
    system.dynamicAccess.getClassFor[Any]("akka.cluster.ddata.typed.scaladsl.DistributedData").isSuccess
  }

  /**
   * akka-cluster-typed dependency is optional so we create the DdataConsumerFilterStore with reflection
   */
  def createDdataConsumerFilterStore(
      system: ActorSystem[_],
      streamId: String,
      notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]): Behavior[Command] = {
    val className = "akka.projection.grpc.internal.DdataConsumerFilterStore"
    system.classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .dynamicAccess
      .getObjectFor[Any]("akka.projection.grpc.internal.DdataConsumerFilterStore") match {
      case Success(companion) =>
        val applyMethod = companion.getClass.getMethod(
          "apply",
          classOf[String],
          classOf[ActorRef[ConsumerFilterRegistry.FilterUpdated]])
        applyMethod.invoke(companion, streamId, notifyUpdatesTo).asInstanceOf[Behavior[Command]]
      case Failure(exc) =>
        LoggerFactory.getLogger(className).error("Couldn't create instance of [{}]", className, exc)
        throw exc
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

  def apply(
      streamId: String,
      notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]): Behavior[ConsumerFilterStore.Command] = {
    Behaviors
      .supervise[ConsumerFilterStore.Command] {
        Behaviors.setup { context =>
          new LocalConsumerFilterStore(context, streamId, notifyUpdatesTo).behavior()
        }
      }
      .onFailure(SupervisorStrategy.restart)
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

      case other =>
        // only to silence compiler due to sealed trait, DdataConsumerFilterStore.InternalCommand
        throw new IllegalStateException(s"Unexpected message [$other]")
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] object DdataConsumerFilterStore {
  object State {
    val empty: State = State(Vector.empty)
  }
  final case class State(filterCriteria: immutable.Seq[ConsumerFilter.FilterCriteria]) extends ReplicatedData {
    // FIXME serialization
    type T = State

    override def merge(that: State): State =
      State(ConsumerFilter.mergeFilter(filterCriteria, that.filterCriteria))

    // FIXME might be easy to implement delta crdt via ConsumerFilter.createDiff
  }

  final case class ConsumerFilterKey(_id: String) extends Key[State](_id) {
    // FIXME serialization

    override def withId(newId: Key.KeyId): ConsumerFilterKey =
      ConsumerFilterKey(newId)
  }

  sealed trait InternalCommand extends ConsumerFilterStore.Command

  private case class InternalGetResponse(
      rsp: Replicator.GetResponse[State],
      replyTo: ActorRef[ConsumerFilter.CurrentFilter])
      extends InternalCommand

  private case class InternalUpdateResponse(rsp: Replicator.UpdateResponse[State]) extends InternalCommand

  private case class InternalSubscribeResponse(chg: Replicator.SubscribeResponse[State]) extends InternalCommand

  def apply(
      streamId: String,
      notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]): Behavior[ConsumerFilterStore.Command] = {
    Behaviors
      .supervise[ConsumerFilterStore.Command] {
        Behaviors.setup { context =>
          val key = ConsumerFilterKey(s"ddataConsumerFilterStore-$streamId")
          DistributedData.withReplicatorMessageAdapter[ConsumerFilterStore.Command, State] { replicatorAdapter =>
            new DdataConsumerFilterStore(context, replicatorAdapter, key, streamId, notifyUpdatesTo).behavior()
          }
        }
      }
      .onFailure(SupervisorStrategy.restart)
  }
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class DdataConsumerFilterStore(
    context: ActorContext[ConsumerFilterStore.Command],
    replicatorAdapter: ReplicatorMessageAdapter[ConsumerFilterStore.Command, DdataConsumerFilterStore.State],
    key: DdataConsumerFilterStore.ConsumerFilterKey,
    streamId: String,
    notifyUpdatesTo: ActorRef[ConsumerFilterRegistry.FilterUpdated]) {
  import ConsumerFilterStore._
  import DdataConsumerFilterStore._

  private val stateReadConsistency = Replicator.ReadMajority(3.seconds) // FIXME config
  private val stateWriteConsistency = Replicator.WriteMajority(3.seconds) // FIXME config

  // FIXME use the expire-keys-after-inactivity for this, and have a keep alive

  replicatorAdapter.subscribe(key, InternalSubscribeResponse.apply)

  def behavior(): Behavior[Command] = {
    Behaviors.receiveMessage {
      case UpdateFilter(updatedCriteria) =>
        replicatorAdapter.askUpdate(
          replyTo => Replicator.Update(key, State.empty, stateWriteConsistency, replyTo)(_ => State(updatedCriteria)),
          response => InternalUpdateResponse(response))
        Behaviors.same

      case InternalUpdateResponse(_ @Replicator.UpdateSuccess(`key`)) =>
        Behaviors.same

      case InternalUpdateResponse(_ @Replicator.UpdateTimeout(`key`)) =>
        // the local write (and maybe some more) would be enough, only need eventual consistency
        context.log.debug("{}: Update timeout", streamId)
        Behaviors.same

      case InternalSubscribeResponse(chg @ Replicator.Changed(`key`)) =>
        val state = chg.get(key)
        notifyUpdatesTo ! ConsumerFilterRegistry.FilterUpdated(streamId, state.filterCriteria)
        Behaviors.same

      case GetFilter(replyTo) =>
        replicatorAdapter.askGet(
          askReplyTo => Replicator.Get(key, stateReadConsistency, askReplyTo),
          response => InternalGetResponse(response, replyTo))
        Behaviors.same

      case InternalGetResponse(success @ Replicator.GetSuccess(`key`), replyTo) =>
        val state = success.get(key)
        replyTo ! ConsumerFilter.CurrentFilter(streamId, state.filterCriteria)
        Behaviors.same

      case InternalGetResponse(_ @Replicator.NotFound(`key`), replyTo) =>
        context.log.debug("{}: No previous state stored", streamId)
        replyTo ! ConsumerFilter.CurrentFilter(streamId, Vector.empty)
        Behaviors.same

      case InternalGetResponse(_ @Replicator.GetFailure(`key`), replyTo) =>
        context.log.debug("{}: Failed fetching state, try again with ReadLocal", streamId)
        // local read (and maybe some more) would be enough, only need eventual consistency
        replicatorAdapter.askGet(
          askReplyTo => Replicator.Get(key, Replicator.ReadLocal, askReplyTo),
          response => InternalGetResponse(response, replyTo))
        Behaviors.same

      case InternalUpdateResponse(other) =>
        throw new IllegalStateException(s"Unexpected UpdateResponse [$other]")

      case InternalSubscribeResponse(other) =>
        throw new IllegalStateException(s"Unexpected SubscribeResponse [$other]")

      case InternalGetResponse(other, _) =>
        throw new IllegalStateException(s"Unexpected GetResponse [$other]")
    }
  }
}
