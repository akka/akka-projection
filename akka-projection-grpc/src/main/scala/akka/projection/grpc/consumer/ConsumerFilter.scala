/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer

import java.util.UUID
import java.util.{ List => JList }
import java.util.{ Set => JSet }

import scala.annotation.tailrec
import scala.collection.immutable
import scala.concurrent.duration.FiniteDuration
import scala.runtime.AbstractFunction2
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Extension
import akka.actor.typed.ExtensionId
import akka.actor.typed.Props
import akka.annotation.InternalApi
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.internal.ConsumerFilterRegistry
import akka.projection.grpc.internal.TopicMatcher
import akka.util.HashCode
import com.typesafe.config.Config

/**
 * Extension to dynamically control the filters for the `GrpcReadJournal`.
 */
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

  object Replay extends AbstractFunction2[String, Set[PersistenceIdOffset], Replay] {
    def apply(streamId: String, persistenceIdOffsets: Set[PersistenceIdOffset]): Replay =
      new Replay(streamId, persistenceIdOffsets, correlationId = None)

    /**
     * Use the `replayCorrelationId` from the `GrpcReadJournal`.
     */
    def apply(streamId: String, persistenceIdOffsets: Set[PersistenceIdOffset], correlationId: UUID): Replay =
      new Replay(streamId, persistenceIdOffsets, Some(correlationId))

    def unapply(arg: Replay): Option[(String, Set[PersistenceIdOffset])] =
      Some((arg.streamId, arg.persistenceIdOffsets))

  }

  /**
   * Explicit request to replay events for given entities.
   *
   * Use the `replayCorrelationId` from the `GrpcReadJournal`.
   */
  final class Replay(
      val streamId: String,
      val persistenceIdOffsets: Set[PersistenceIdOffset],
      val correlationId: Option[UUID])
      extends Product2[String, Set[PersistenceIdOffset]] // for binary compatibility (used to be a case class)
      with SubscriberCommand
      with Serializable {

    // for binary compatibility (used to be a case class)
    def this(streamId: String, persistenceIdOffsets: Set[PersistenceIdOffset]) =
      this(streamId, persistenceIdOffsets, None)

    /** Java API */
    def this(streamId: String, persistenceIdOffsets: JSet[PersistenceIdOffset]) =
      this(streamId, persistenceIdOffsets.asScala.toSet, None)

    /**
     * Java API
     *
     * Use the `replayCorrelationId` from the `GrpcReadJournal`.
     */
    def this(streamId: String, persistenceIdOffsets: JSet[PersistenceIdOffset], correlationId: UUID) =
      this(streamId, persistenceIdOffsets.asScala.toSet, Some(correlationId))

    def toReplayWithFilter: ReplayWithFilter =
      new ReplayWithFilter(
        streamId,
        persistenceIdOffsets.map(p => ReplayPersistenceId(p, filterAfterSeqNr = Long.MaxValue)),
        correlationId)

    override def hashCode(): Int = {
      var result = HashCode.SEED
      result = HashCode.hash(result, streamId)
      result = HashCode.hash(result, persistenceIdOffsets)
      result = HashCode.hash(result, correlationId)
      result
    }

    override def equals(obj: Any): Boolean = obj match {
      case other: Replay =>
        streamId == other.streamId && persistenceIdOffsets == other.persistenceIdOffsets && correlationId == other.correlationId
      case _ => false
    }

    override def toString: String =
      s"Replay($streamId,$persistenceIdOffsets,$correlationId)"

    // for binary compatibility (used to be a case class)
    def copy(
        streamId: String = streamId,
        persistenceIdOffsets: Set[PersistenceIdOffset] = persistenceIdOffsets): Replay =
      new Replay(streamId, persistenceIdOffsets, correlationId)

    // Product2, for binary compatibility (used to be a case class)
    override def productPrefix = "Replay"
    override def _1: String = streamId
    override def _2: Set[PersistenceIdOffset] = persistenceIdOffsets
    override def canEqual(that: Any): Boolean = that.isInstanceOf[Replay]
  }

  object ReplayWithFilter extends AbstractFunction2[String, Set[ReplayPersistenceId], ReplayWithFilter] {
    def apply(streamId: String, replayPersistenceIds: Set[ReplayPersistenceId]): ReplayWithFilter =
      new ReplayWithFilter(streamId, replayPersistenceIds, correlationId = None)

    /**
     * Use the `replayCorrelationId` from the `GrpcReadJournal`.
     */
    def apply(streamId: String, replayPersistenceIds: Set[ReplayPersistenceId], correlationId: UUID): ReplayWithFilter =
      new ReplayWithFilter(streamId, replayPersistenceIds, Some(correlationId))

    def unapply(arg: ReplayWithFilter): Option[(String, Set[ReplayPersistenceId])] =
      Some((arg.streamId, arg.replayPersistenceIds))

  }

  /**
   * Explicit request to replay events for given entities.
   *
   * Use the `replayCorrelationId` from the `GrpcReadJournal`.
   */
  final class ReplayWithFilter(
      val streamId: String,
      val replayPersistenceIds: Set[ReplayPersistenceId],
      val correlationId: Option[UUID])
      extends Product2[String, Set[ReplayPersistenceId]] // for binary compatibility (used to be a case class)
      with SubscriberCommand
      with Serializable {

    // for binary compatibility (used to be a case class)
    def this(streamId: String, replayPersistenceIds: Set[ReplayPersistenceId]) =
      this(streamId, replayPersistenceIds, None)

    /** Java API */
    def this(streamId: String, persistenceIdOffsets: JSet[ReplayPersistenceId]) =
      this(streamId, persistenceIdOffsets.asScala.toSet, None)

    /**
     * Java API
     *
     * Use the `replayCorrelationId` from the `GrpcReadJournal`.
     */
    def this(streamId: String, persistenceIdOffsets: JSet[ReplayPersistenceId], correlationId: UUID) =
      this(streamId, persistenceIdOffsets.asScala.toSet, Some(correlationId))

    override def hashCode(): Int = {
      var result = HashCode.SEED
      result = HashCode.hash(result, streamId)
      result = HashCode.hash(result, replayPersistenceIds)
      result = HashCode.hash(result, correlationId)
      result
    }

    override def equals(obj: Any): Boolean = obj match {
      case other: ReplayWithFilter =>
        streamId == other.streamId && replayPersistenceIds == other.replayPersistenceIds && correlationId == other.correlationId
      case _ => false
    }

    override def toString: String =
      s"ReplayWithFilter($streamId,$replayPersistenceIds,$correlationId)"

    // for binary compatibility (used to be a case class)
    def copy(
        streamId: String = streamId,
        replayPersistenceIds: Set[ReplayPersistenceId] = replayPersistenceIds): ReplayWithFilter =
      new ReplayWithFilter(streamId, replayPersistenceIds, correlationId)

    // Product2, for binary compatibility (used to be a case class)
    override def productPrefix = "ReplayWithFilter"
    override def _1: String = streamId
    override def _2: Set[ReplayPersistenceId] = replayPersistenceIds
    override def canEqual(that: Any): Boolean = that.isInstanceOf[ReplayWithFilter]
  }

  sealed trait FilterCriteria
  sealed trait RemoveCriteria extends FilterCriteria

  /**
   * Exclude events from all entity ids, convenience for combining with for example a topic filter
   * to include only events matching the topic filter.
   */
  val excludeAll: FilterCriteria = ExcludeRegexEntityIds(Set(".*"))

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
   * Include events with any of the given matching topics. A matching include overrides a matching exclude.
   *
   * Topic match expression according to MQTT specification, including wildcards.
   * The topic of an event is defined by a tag with certain prefix, see `topic-tag-prefix` configuration.
   */
  final case class IncludeTopics(expressions: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(expressions: JSet[String]) =
      this(expressions.asScala.toSet)

    expressions.foreach(TopicMatcher.checkValid)
  }

  /**
   * Remove a previously added [[IncludeTopics]].
   */
  final case class RemoveIncludeTopics(expressions: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(expressions: JSet[String]) =
      this(expressions.asScala.toSet)
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

  /**
   * Include events for entities with entity ids matching the given regular expressions.
   * A matching include overrides a matching exclude.
   */
  final case class IncludeRegexEntityIds(matching: Set[String]) extends FilterCriteria {

    /** Java API */
    def this(matching: JSet[String]) =
      this(matching.asScala.toSet)
  }

  /**
   * Remove a previously added [[IncludeRegexEntityIds]].
   */
  final case class RemoveIncludeRegexEntityIds(matching: Set[String]) extends RemoveCriteria {

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

  final case class ReplayPersistenceId(persistenceIdOffset: PersistenceIdOffset, filterAfterSeqNr: Long)

  override def createExtension(system: ActorSystem[_]): ConsumerFilter = new ConsumerFilter(system)

  /**
   * Java API: retrieve the extension instance for the given system.
   */
  def get(system: ActorSystem[_]): ConsumerFilter = apply(system)

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

    val removeIncludeTopics =
      both.flatMap {
        case rem: RemoveIncludeTopics => rem.expressions
        case _                        => Set.empty[String]
      }.toSet
    val includeTopics2 = includeTopics(both).diff(removeIncludeTopics)

    val removeExcludeRegexEntityIds =
      both.flatMap {
        case rem: RemoveExcludeRegexEntityIds => rem.matching
        case _                                => Set.empty[String]
      }.toSet
    val excludeRegexEntityIds2 = excludeRegexEntityIds(both).diff(removeExcludeRegexEntityIds)

    val removeIncludeRegexEntityIds =
      both.flatMap {
        case rem: RemoveIncludeRegexEntityIds => rem.matching
        case _                                => Set.empty[String]
      }.toSet
    val includeRegexEntityIds2 = includeRegexEntityIds(both).diff(removeIncludeRegexEntityIds)

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
      if (includeTopics2.isEmpty) None else Some(IncludeTopics(includeTopics2)),
      if (excludeRegexEntityIds2.isEmpty) None else Some(ExcludeRegexEntityIds(excludeRegexEntityIds2)),
      if (includeRegexEntityIds2.isEmpty) None else Some(IncludeRegexEntityIds(includeRegexEntityIds2)),
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

    val includeTopicsA = includeTopics(a)
    val includeTopicsB = includeTopics(b)
    val includeTopicsDiffAB = includeTopicsA.diff(includeTopicsB)
    val includeTopicsDiffBA = includeTopicsB.diff(includeTopicsA)
    val includeTopicsCriteria =
      if (includeTopicsDiffBA.isEmpty) None else Some(IncludeTopics(includeTopicsDiffBA))
    val removeIncludeTopicsCriteria =
      if (includeTopicsDiffAB.isEmpty) None else Some(RemoveIncludeTopics(includeTopicsDiffAB))

    val excludeRegexEntityIdsA = excludeRegexEntityIds(a)
    val excludeRegexEntityIdsB = excludeRegexEntityIds(b)
    val excludeRegexEntityIdsDiffAB = excludeRegexEntityIdsA.diff(excludeRegexEntityIdsB)
    val excludeRegexEntityIdsDiffBA = excludeRegexEntityIdsB.diff(excludeRegexEntityIdsA)
    val excludeRegexEntityIdCriteria =
      if (excludeRegexEntityIdsDiffBA.isEmpty) None else Some(ExcludeRegexEntityIds(excludeRegexEntityIdsDiffBA))
    val removeExcludeRegexEntityIdCriteria =
      if (excludeRegexEntityIdsDiffAB.isEmpty) None else Some(RemoveExcludeRegexEntityIds(excludeRegexEntityIdsDiffAB))

    val includeRegexEntityIdsA = includeRegexEntityIds(a)
    val includeRegexEntityIdsB = includeRegexEntityIds(b)
    val includeRegexEntityIdsDiffAB = includeRegexEntityIdsA.diff(includeRegexEntityIdsB)
    val includeRegexEntityIdsDiffBA = includeRegexEntityIdsB.diff(includeRegexEntityIdsA)
    val includeRegexEntityIdCriteria =
      if (includeRegexEntityIdsDiffBA.isEmpty) None else Some(IncludeRegexEntityIds(includeRegexEntityIdsDiffBA))
    val removeIncludeRegexEntityIdCriteria =
      if (includeRegexEntityIdsDiffAB.isEmpty) None else Some(RemoveIncludeRegexEntityIds(includeRegexEntityIdsDiffAB))

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
      includeTopicsCriteria,
      removeIncludeTopicsCriteria,
      excludeRegexEntityIdCriteria,
      removeExcludeRegexEntityIdCriteria,
      includeRegexEntityIdCriteria,
      removeIncludeRegexEntityIdCriteria,
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
  @InternalApi private[akka] def includeTopics(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case incl: IncludeTopics => incl.expressions
      case _                   => Set.empty[String]
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
  @InternalApi private[akka] def includeRegexEntityIds(filter: immutable.Seq[FilterCriteria]): Set[String] = {
    filter.flatMap {
      case rxp: IncludeRegexEntityIds => rxp.matching
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
        ddataReadTimeout = config.getDuration("ddata-read-timeout").toScala,
        ddataWriteTimeout = config.getDuration("ddata-write-timeout").toScala)
  }

  /** INTERNAL API */
  @InternalApi private[akka] final case class ConsumerFilterSettings(
      ddataReadTimeout: FiniteDuration,
      ddataWriteTimeout: FiniteDuration)

}

final class ConsumerFilter(system: ActorSystem[_]) extends Extension {

  private val settings = ConsumerFilter.ConsumerFilterSettings(system)

  val ref: ActorRef[ConsumerFilter.Command] =
    system.systemActorOf(ConsumerFilterRegistry(settings), "projectionGrpcConsumerFilter", Props.empty)

}
