/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import java.util.{ List => JList }
import java.util.{ Set => JSet }

import scala.collection.immutable

import akka.util.ccompat.JavaConverters._
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.annotation.InternalApi
import akka.projection.grpc.internal.ConsumerFilterRegistry

/**
 * Extension to dynamically control the filters for the `GrpcReadJournal`.
 */
object ConsumerFilter extends ExtensionId[ConsumerFilter] {
  trait Command
  sealed trait SubscriberCommand extends Command {
    def streamId: String
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] final case class Subscribe(streamId: String, subscriber: ActorRef[SubscriberCommand])
      extends Command

  /**
   * Add or remove filter criteria.
   *
   * Exclude criteria are evaluated first.
   * If no matching exclude the event is emitted.
   * If an exclude is matching the include criteria are evaluated.
   * If no matching include the event is discarded.
   * If matching include the event is emitted.
   */
  final case class UpdateFilter(streamId: String, criteria: immutable.Seq[FilterCriteria]) extends SubscriberCommand {

    /** Java API */
    def this(streamId: String, criteria: JList[FilterCriteria]) =
      this(streamId, criteria.asScala.toVector)
  }

  final case class GetFilter(streamId: String, replyTo: ActorRef[CurrentFilter]) extends Command

  final case class CurrentFilter(streamId: String, criteria: immutable.Seq[FilterCriteria]) {

    /** Java API */
    def getCriteria(): JList[FilterCriteria] =
      criteria.asJava
  }

  /**
   * Explicit request to replay events for given entities.
   */
  final case class Replay(streamId: String, entityOffsets: Set[EntityIdOffset]) extends SubscriberCommand {

    /** Java API */
    def this(streamId: String, entityOffsets: JSet[EntityIdOffset]) =
      this(streamId, entityOffsets.asScala.toSet)
  }

  sealed trait FilterCriteria

  /**
   * Exclude events for entities with entity ids matching the given regular expressions,
   * unless there is a matching include filter that overrides the exclude.
   */
  final case class ExcludeRegexEntityIds(matching: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(matching: JSet[String]) =
      this(matching.asScala.toSet)
  }

  /**
   * Remove a previously added [[ExcludeRegexEntityIds]].
   */
  final case class RemoveExcludeRegexEntityIds(matching: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(matching: JSet[String]) =
      this(matching.asScala.toSet)
  }

  /**
   * Exclude events for entities with the given entity ids,
   * unless there is a matching include filter that overrides the exclude.
   */
  final case class ExcludeEntityIds(entityIds: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)
  }

  /**
   * Remove a previously added [[ExcludeEntityIds]].
   */
  final case class RemoveExcludeEntityIds(entityIds: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)
  }

  /**
   * Include events for entities with the given entity ids. A matching include overrides
   * a matching exclude.
   *
   * For the given entity ids a `seqNr` can be defined to replay all events for the entity
   * from the sequence number (inclusive). If `seqNr` is 0 events will not be replayed.
   */
  final case class IncludeEntityIds(entityOffsets: Set[EntityIdOffset]) extends FilterCriteria {

    /** Java API */
    def this(entityOffsets: JSet[EntityIdOffset]) =
      this(entityOffsets.asScala.toSet)
  }

  /**
   * Remove a previously added [[IncludeEntityIds]].
   */
  final case class RemoveIncludeEntityIds(entityIds: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)
  }

  final case class EntityIdOffset(entityId: String, seqNr: Long)

  override def createExtension(system: ActorSystem[_]): ConsumerFilter = new ConsumerFilter(system)

}

class ConsumerFilter(system: ActorSystem[_]) extends Extension {

  val ref: ActorRef[ConsumerFilter.Command] =
    system.systemActorOf(ConsumerFilterRegistry(), "projectionGrpcConsumerFilter", Props.empty)

}
