/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import java.util.{ List => JList }
import java.util.{ Set => JSet }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.collection.immutable.Set

import akka.util.ccompat.JavaConverters._
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.annotation.InternalApi
import akka.projection.grpc.internal.ConsumerFilterRegistry

// FIXME add ApiMayChange in all places

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
   *
   * Before registering the subscriber it would retrieve the current filter with `GetFilter`.
   * That init filter is then included in the `Subscribe.initCriteria`, to be used as starting point for evaluating
   * the diff for that subscriber.
   */
  @InternalApi private[akka] final case class Subscribe(
      streamId: String,
      initCriteria: immutable.Seq[FilterCriteria],
      subscriber: ActorRef[SubscriberCommand])
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

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def mergeFilter(
      currentFilter: immutable.Seq[FilterCriteria],
      update: immutable.Seq[FilterCriteria]): immutable.Seq[FilterCriteria] = {

    val both = currentFilter ++ update

    val removeExcludeRegexEntityIds =
      both.flatMap {
        case rem: RemoveExcludeRegexEntityIds => rem.matching
        case _                                => Set.empty
      }.toSet
    val excludeRegexEntityIds2 = excludeRegexEntityIds(both).diff(removeExcludeRegexEntityIds)

    val removeExcludeEntityIds =
      both.flatMap {
        case rem: RemoveExcludeEntityIds => rem.entityIds
        case _                           => Set.empty
      }.toSet
    val excludeEntityIds2 = excludeEntityIds(both).diff(removeExcludeEntityIds)

    val removeIncludeEntityIds =
      both.flatMap {
        case rem: RemoveIncludeEntityIds => rem.entityIds
        case _                           => Set.empty
      }.toSet
    val includeEntityOffsets2 = includeEntityOffsets(both).filterNot(x => removeIncludeEntityIds.contains(x.entityId))
    val includeEntityOffsets3 = deduplicateEntityOffsets(includeEntityOffsets2.iterator, Map.empty)

    Vector(
      if (excludeRegexEntityIds2.isEmpty) None else Some(ExcludeRegexEntityIds(excludeRegexEntityIds2)),
      if (excludeEntityIds2.isEmpty) None else Some(ExcludeEntityIds(excludeEntityIds2)),
      if (includeEntityOffsets3.isEmpty) None else Some(IncludeEntityIds(includeEntityOffsets3))).flatten
  }

  // remove duplicates, use highest seqNr
  @tailrec private def deduplicateEntityOffsets(
      entityOffsets: Iterator[EntityIdOffset],
      result: Map[String, EntityIdOffset]): Set[EntityIdOffset] = {
    if (entityOffsets.hasNext) {
      val offset = entityOffsets.next()
      result.get(offset.entityId) match {
        case None =>
          deduplicateEntityOffsets(entityOffsets, result.updated(offset.entityId, offset))
        case Some(o) =>
          if (offset.seqNr > o.seqNr)
            deduplicateEntityOffsets(entityOffsets, result.updated(offset.entityId, offset))
          else
            deduplicateEntityOffsets(entityOffsets, result)
      }
    } else {
      result.values.toSet
    }
  }

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def createDiff(
      a: immutable.Seq[FilterCriteria],
      b: immutable.Seq[FilterCriteria]): immutable.Seq[FilterCriteria] = {

    val excludeRegexEntityIdsA = excludeRegexEntityIds(a)
    val excludeRegexEntityIdsB = excludeRegexEntityIds(b)
    val excludeRegexEntityIdsDiffAB = excludeRegexEntityIdsA.diff(excludeRegexEntityIdsB)
    val excludeRegexEntityIdsDiffBA = excludeRegexEntityIdsB.diff(excludeRegexEntityIdsA)
    val excludeRegexEntityIdCriteria =
      if (excludeRegexEntityIdsDiffBA.isEmpty) None else Some(ExcludeRegexEntityIds(excludeRegexEntityIdsDiffBA))
    val removeExcludeRegexEntityIdCriteria =
      if (excludeRegexEntityIdsDiffAB.isEmpty) None else Some(RemoveExcludeRegexEntityIds(excludeRegexEntityIdsDiffAB))

    val excludeEntityIdsA = excludeEntityIds(a)
    val excludeEntityIdsB = excludeEntityIds(b)
    val excludeEntityIdsDiffAB = excludeEntityIdsA.diff(excludeEntityIdsB)
    val excludeEntityIdsDiffBA = excludeEntityIdsB.diff(excludeEntityIdsA)
    val excludeEntityIdCriteria =
      if (excludeEntityIdsDiffBA.isEmpty) None else Some(ExcludeEntityIds(excludeEntityIdsDiffBA))
    val removeExcludeEntityIdCriteria =
      if (excludeEntityIdsDiffAB.isEmpty) None else Some(RemoveExcludeEntityIds(excludeEntityIdsDiffAB))

    val includeEntityOffsetsA = includeEntityOffsets(a).map(x => x.entityId -> x).toMap
    val includeEntityOffsetsB = includeEntityOffsets(b).map(x => x.entityId -> x).toMap
    val includeEntityOffsetsDiffAB = includeEntityOffsetsA.filter {
      case (entityId, _) => !includeEntityOffsetsB.contains(entityId)
    }
    val includeEntityOffsetsDiffBA = includeEntityOffsetsB.filter {
      case (entityId, offsetB) =>
        includeEntityOffsetsA.get(entityId) match {
          case None          => true
          case Some(offsetA) => offsetB.seqNr > offsetA.seqNr
        }
    }
    val includeEntityIdCriteria =
      if (includeEntityOffsetsDiffBA.isEmpty) None else Some(IncludeEntityIds(includeEntityOffsetsDiffBA.values.toSet))
    val removeIncludeEntityIdCriteria =
      if (includeEntityOffsetsDiffAB.isEmpty) None else Some(RemoveIncludeEntityIds(includeEntityOffsetsDiffAB.keySet))

    Vector(
      excludeRegexEntityIdCriteria,
      removeExcludeRegexEntityIdCriteria,
      excludeEntityIdCriteria,
      removeExcludeEntityIdCriteria,
      includeEntityIdCriteria,
      removeIncludeEntityIdCriteria).flatten

  }

  private def includeEntityOffsets(filter: Seq[FilterCriteria]): Set[EntityIdOffset] = {
    filter.flatMap {
      case inc: IncludeEntityIds => inc.entityOffsets
      case _                     => Set.empty
    }.toSet
  }

  private def excludeEntityIds(filter: Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case exl: ExcludeEntityIds => exl.entityIds
      case _                     => Set.empty
    }.toSet
  }

  private def excludeRegexEntityIds(filter: Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case rxp: ExcludeRegexEntityIds => rxp.matching
      case _                          => Set.empty
    }.toSet
  }

}

class ConsumerFilter(system: ActorSystem[_]) extends Extension {

  val ref: ActorRef[ConsumerFilter.Command] =
    system.systemActorOf(ConsumerFilterRegistry(), "projectionGrpcConsumerFilter", Props.empty)

}
