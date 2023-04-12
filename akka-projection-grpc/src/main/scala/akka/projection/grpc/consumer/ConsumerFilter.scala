/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer

import java.util.{ List => JList }
import java.util.{ Set => JSet }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.internal.ConsumerFilterRegistry
import akka.util.JavaDurationConverters._
import akka.util.ccompat.JavaConverters._
import com.typesafe.config.Config

/**
 * Extension to dynamically control the filters for the `GrpcReadJournal`.
 */
@ApiMayChange
object ConsumerFilter extends ExtensionId[ConsumerFilter] {

  private val ReplicationIdSeparator = '|'
  private val ToStringLimit = 100

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
   * If no matching exclude criteria the event is emitted.
   * If an exclude criteria is matching the include criteria are evaluated.
   * If no matching include criteria the event is discarded.
   * If matching include criteria the event is emitted.
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
  final case class Replay(streamId: String, persistenceIdOffsets: Set[PersistenceIdOffset]) extends SubscriberCommand {

    /** Java API */
    def this(streamId: String, persistenceIdOffsets: JSet[PersistenceIdOffset]) =
      this(streamId, persistenceIdOffsets.asScala.toSet)
  }

  sealed trait FilterCriteria
  sealed trait RemoveCriteria extends FilterCriteria

  /**
   * Exclude events with any of the given tags,
   * unless there is a matching include filter that overrides the exclude.
   */
  final case class ExcludeTags(tags: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(tags: JSet[String]) =
      this(tags.asScala.toSet)
  }

  /**
   * Remove a previously added [[ExcludeTags]].
   */
  final case class RemoveExcludeTags(tags: Set[String]) extends RemoveCriteria {

    /** Java API */
    def this(tags: JSet[String]) =
      this(tags.asScala.toSet)
  }

  /**
   * Include events with any of the given tags. A matching include overrides
   * a matching exclude.
   */
  final case class IncludeTags(tags: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(tags: JSet[String]) =
      this(tags.asScala.toSet)
  }

  /**
   * Remove a previously added [[IncludeTags]].
   */
  final case class RemoveIncludeTags(tags: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(tags: JSet[String]) =
      this(tags.asScala.toSet)
  }

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
  final case class RemoveExcludeRegexEntityIds(matching: Set[String]) extends RemoveCriteria {

    /** Java API */
    def this(matching: JSet[String]) =
      this(matching.asScala.toSet)
  }

  object ExcludeEntityIds {
    def apply(replicaId: ReplicaId, entityIds: Set[String]): ExcludeEntityIds =
      ExcludeEntityIds(entityIds.map(addReplicaIdToEntityId(replicaId, _)))
  }

  /**
   * Exclude events for entities with the given entity ids,
   * unless there is a matching include filter that overrides the exclude.
   */
  final case class ExcludeEntityIds(entityIds: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)

    override def toString: String = {
      if (entityIds.size > ToStringLimit)
        s"ExcludeEntityIds(${entityIds.take(ToStringLimit).mkString(", ")} ...)"
      else
        s"ExcludeEntityIds(${entityIds.mkString(", ")})"
    }
  }

  object RemoveExcludeEntityIds {
    def apply(replicaId: ReplicaId, entityIds: Set[String]): RemoveExcludeEntityIds =
      RemoveExcludeEntityIds(entityIds.map(addReplicaIdToEntityId(replicaId, _)))
  }

  /**
   * Remove a previously added [[ExcludeEntityIds]].
   */
  final case class RemoveExcludeEntityIds(entityIds: Set[String]) extends RemoveCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)

    override def toString: String = {
      if (entityIds.size > ToStringLimit)
        s"RemoveExcludeEntityIds(${entityIds.take(ToStringLimit).mkString(", ")} ...)"
      else
        s"RemoveExcludeEntityIds(${entityIds.mkString(", ")})"
    }
  }

  object IncludeEntityIds {
    def apply(replicaId: ReplicaId, entityOffsets: Set[EntityIdOffset]): IncludeEntityIds =
      IncludeEntityIds(
        entityOffsets.map(offset => EntityIdOffset(addReplicaIdToEntityId(replicaId, offset.entityId), offset.seqNr)))
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

    override def toString: String = {
      if (entityOffsets.size > ToStringLimit)
        s"IncludeEntityIds(${entityOffsets.take(ToStringLimit).mkString(", ")} ...)"
      else
        s"IncludeEntityIds(${entityOffsets.mkString(", ")})"
    }
  }

  object RemoveIncludeEntityIds {
    def apply(replicaId: ReplicaId, entityIds: Set[String]): RemoveIncludeEntityIds =
      RemoveIncludeEntityIds(entityIds.map(addReplicaIdToEntityId(replicaId, _)))
  }

  /**
   * Remove a previously added [[IncludeEntityIds]].
   */
  final case class RemoveIncludeEntityIds(entityIds: Set[String]) extends RemoveCriteria {

    /** Java API */
    def this(entityIds: JSet[String]) =
      this(entityIds.asScala.toSet)

    override def toString: String = {
      if (entityIds.size > ToStringLimit)
        s"RemoveIncludeEntityIds(${entityIds.take(ToStringLimit).mkString(", ")} ...)"
      else
        s"RemoveIncludeEntityIds(${entityIds.mkString(", ")})"
    }
  }

  private def addReplicaIdToEntityId(replicaId: ReplicaId, entityId: String): String =
    s"$entityId$ReplicationIdSeparator${replicaId.id}"

  final case class EntityIdOffset(entityId: String, seqNr: Long)

  final case class PersistenceIdOffset(persistenceIdId: String, seqNr: Long)

  override def createExtension(system: ActorSystem[_]): ConsumerFilter = new ConsumerFilter(system)

  /**
   * INTERNAL API
   */
  @InternalApi private[akka] def mergeFilter(
      currentFilter: immutable.Seq[FilterCriteria],
      update: immutable.Seq[FilterCriteria]): immutable.Seq[FilterCriteria] = {

    val both = currentFilter ++ update

    val removeExcludeTags =
      both.flatMap {
        case rem: RemoveExcludeTags => rem.tags
        case _                      => Set.empty[String]
      }.toSet
    val excludeTags2 = excludeTags(both).diff(removeExcludeTags)

    val removeIncludeTags =
      both.flatMap {
        case rem: RemoveIncludeTags => rem.tags
        case _                      => Set.empty[String]
      }.toSet
    val includeTags2 = includeTags(both).diff(removeIncludeTags)

    val removeExcludeRegexEntityIds =
      both.flatMap {
        case rem: RemoveExcludeRegexEntityIds => rem.matching
        case _                                => Set.empty[String]
      }.toSet
    val excludeRegexEntityIds2 = excludeRegexEntityIds(both).diff(removeExcludeRegexEntityIds)

    val removeExcludeEntityIds =
      both.flatMap {
        case rem: RemoveExcludeEntityIds => rem.entityIds
        case _                           => Set.empty[String]
      }.toSet
    val excludeEntityIds2 = excludeEntityIds(both).diff(removeExcludeEntityIds)

    val removeIncludeEntityIds =
      both.flatMap {
        case rem: RemoveIncludeEntityIds => rem.entityIds
        case _                           => Set.empty[String]
      }.toSet
    val includeEntityOffsets2 = includeEntityOffsets(both).filterNot(x => removeIncludeEntityIds.contains(x.entityId))
    val includeEntityOffsets3 = deduplicateEntityOffsets(includeEntityOffsets2.iterator, Map.empty)

    Vector(
      if (excludeTags2.isEmpty) None else Some(ExcludeTags(excludeTags2)),
      if (includeTags2.isEmpty) None else Some(IncludeTags(includeTags2)),
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

    require(!hasRemoveCriteria(a), "Unexpected RemoveCriteria in a when creating diff, use mergeFilter first.")
    require(!hasRemoveCriteria(b), "Unexpected RemoveCriteria in b when creating diff, use mergeFilter first.")

    val excludeTagsA = excludeTags(a)
    val excludeTagsB = excludeTags(b)
    val excludeTagsDiffAB = excludeTagsA.diff(excludeTagsB)
    val excludeTagsDiffBA = excludeTagsB.diff(excludeTagsA)
    val excludeTagsCriteria =
      if (excludeTagsDiffBA.isEmpty) None else Some(ExcludeTags(excludeTagsDiffBA))
    val removeExcludeTagsCriteria =
      if (excludeTagsDiffAB.isEmpty) None else Some(RemoveExcludeTags(excludeTagsDiffAB))

    val includeTagsA = includeTags(a)
    val includeTagsB = includeTags(b)
    val includeTagsDiffAB = includeTagsA.diff(includeTagsB)
    val includeTagsDiffBA = includeTagsB.diff(includeTagsA)
    val includeTagsCriteria =
      if (includeTagsDiffBA.isEmpty) None else Some(IncludeTags(includeTagsDiffBA))
    val removeIncludeTagsCriteria =
      if (includeTagsDiffAB.isEmpty) None else Some(RemoveIncludeTags(includeTagsDiffAB))

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
      excludeTagsCriteria,
      removeExcludeTagsCriteria,
      includeTagsCriteria,
      removeIncludeTagsCriteria,
      excludeRegexEntityIdCriteria,
      removeExcludeRegexEntityIdCriteria,
      excludeEntityIdCriteria,
      removeExcludeEntityIdCriteria,
      includeEntityIdCriteria,
      removeIncludeEntityIdCriteria).flatten

  }

  /** INTERNAL API */
  @InternalApi private[akka] def includeEntityOffsets(filter: immutable.Seq[FilterCriteria]): Set[EntityIdOffset] = {
    filter.flatMap {
      case inc: IncludeEntityIds => inc.entityOffsets
      case _                     => Set.empty[EntityIdOffset]
    }.toSet
  }

  /** INTERNAL API */
  @InternalApi private[akka] def excludeTags(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case exl: ExcludeTags => exl.tags
      case _                => Set.empty[String]
    }.toSet
  }

  /** INTERNAL API */
  @InternalApi private[akka] def includeTags(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case incl: IncludeTags => incl.tags
      case _                 => Set.empty[String]
    }.toSet
  }

  /** INTERNAL API */
  @InternalApi private[akka] def excludeEntityIds(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case exl: ExcludeEntityIds => exl.entityIds
      case _                     => Set.empty[String]
    }.toSet
  }

  /** INTERNAL API */
  @InternalApi private[akka] def excludeRegexEntityIds(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case rxp: ExcludeRegexEntityIds => rxp.matching
      case _                          => Set.empty[String]
    }.toSet
  }

  /** INTERNAL API */
  @InternalApi private[akka] def hasRemoveCriteria(filter: immutable.Seq[FilterCriteria]): Boolean =
    filter.exists(_.isInstanceOf[RemoveCriteria])

  /** INTERNAL API */
  @InternalApi private[akka] object ConsumerFilterSettings {
    def apply(system: ActorSystem[_]): ConsumerFilterSettings =
      apply(system.settings.config.getConfig("akka.projection.grpc.consumer.filter"))

    def apply(config: Config): ConsumerFilterSettings =
      ConsumerFilterSettings(
        ddataReadTimeout = config.getDuration("ddata-read-timeout").asScala,
        ddataWriteTimeout = config.getDuration("ddata-write-timeout").asScala)
  }

  /** INTERNAL API */
  @InternalApi private[akka] final case class ConsumerFilterSettings(
      ddataReadTimeout: FiniteDuration,
      ddataWriteTimeout: FiniteDuration)

}

@ApiMayChange
class ConsumerFilter(system: ActorSystem[_]) extends Extension {

  private val settings = ConsumerFilter.ConsumerFilterSettings(system)

  val ref: ActorRef[ConsumerFilter.Command] =
    system.systemActorOf(ConsumerFilterRegistry(settings), "projectionGrpcConsumerFilter", Props.empty)

}
