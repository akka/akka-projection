/*
 * Copyright (C) 2023-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex
import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.FilteredPayload
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.ReplayPersistenceId
import akka.projection.grpc.internal.proto.StreamIn
import akka.stream.Attributes
import akka.stream.BidiShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.SinkQueueWithCancel
import akka.stream.scaladsl.Source
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FilterStage {
  private val ReplicationIdSeparator = '|'

  object Filter {
    def empty(topicTagPrefix: String): Filter =
      Filter(Set.empty, Set.empty, Set.empty, topicTagPrefix, Set.empty, Set.empty, Map.empty, Map.empty)
  }

  final case class Filter(
      includeTags: Set[String],
      excludeTags: Set[String],
      includeTopics: Set[String],
      topicTagPrefix: String,
      includePersistenceIds: Set[String],
      excludePersistenceIds: Set[String],
      includeRegexEntityIds: Map[String, Regex],
      excludeRegexEntityIds: Map[String, Regex]) {

    def addIncludeTags(tags: Iterable[String]): Filter =
      copy(includeTags = includeTags ++ tags)

    def removeIncludeTags(tags: Iterable[String]): Filter =
      copy(includeTags = includeTags -- tags)

    def addExcludeTags(tags: Iterable[String]): Filter =
      copy(excludeTags = excludeTags ++ tags)

    def removeExcludeTags(tags: Iterable[String]): Filter =
      copy(excludeTags = excludeTags -- tags)

    def addIncludeTopics(expressions: Iterable[String]): Filter =
      copy(includeTopics = includeTopics ++ expressions)

    def removeIncludeTopics(expressions: Iterable[String]): Filter =
      copy(includeTopics = includeTopics -- expressions)

    def addIncludePersistenceIds(pids: Iterable[String]): Filter =
      copy(includePersistenceIds = includePersistenceIds ++ pids)

    def removeIncludePersistenceIds(pids: Iterable[String]): Filter =
      copy(includePersistenceIds = includePersistenceIds -- pids)

    def addExcludePersistenceIds(pids: Iterable[String]): Filter =
      copy(excludePersistenceIds = excludePersistenceIds ++ pids)

    def removeExcludePersistenceIds(pids: Iterable[String]): Filter =
      copy(excludePersistenceIds = excludePersistenceIds -- pids)

    def addExcludeRegexEntityIds(reqexStr: Iterable[String]): Filter =
      copy(excludeRegexEntityIds = excludeRegexEntityIds ++ reqexStr.map(s => s -> s.r))

    def removeExcludeRegexEntityIds(reqexStr: Iterable[String]): Filter =
      copy(excludeRegexEntityIds = excludeRegexEntityIds -- reqexStr)

    def addIncludeRegexEntityIds(reqexStr: Iterable[String]): Filter =
      copy(includeRegexEntityIds = includeRegexEntityIds ++ reqexStr.map(s => s -> s.r))

    def removeIncludeRegexEntityIds(reqexStr: Iterable[String]): Filter =
      copy(includeRegexEntityIds = includeRegexEntityIds -- reqexStr)

    private val topicMatchers = includeTopics.iterator.map(TopicMatcher(_)).toVector

    /**
     * Exclude criteria are evaluated first.
     * Returns `true` if no matching exclude criteria.
     * If an exclude criteria is matching the include criteria are evaluated.
     * Returns `true` if there is a matching include criteria, otherwise `false`.
     */
    def matches(env: EventEnvelope[_]): Boolean = {
      val pid = env.persistenceId

      def matchesRegexEntityIds(regexValues: Iterable[Regex]): Boolean = {
        val entityId = PersistenceId.extractEntityId(pid)
        regexValues.exists {
          _.pattern.matcher(entityId).matches()
        }
      }

      def matchesExcludeRegexEntityIds: Boolean =
        matchesRegexEntityIds(excludeRegexEntityIds.values)

      def matchesIncludeRegexEntityIds: Boolean =
        matchesRegexEntityIds(includeRegexEntityIds.values)

      def matchesTopics: Boolean =
        topicMatchers.exists(_.matches(env, topicTagPrefix))

      if (env.tags.intersect(excludeTags).nonEmpty ||
          excludePersistenceIds.contains(pid) ||
          matchesExcludeRegexEntityIds) {
        env.tags.intersect(includeTags).nonEmpty ||
        matchesTopics ||
        includePersistenceIds.contains(pid) ||
        matchesIncludeRegexEntityIds
      } else {
        true
      }
    }
  }

  private case class ReplayEnvelope(persistenceId: String, env: Option[EventEnvelope[Any]])

  private case class ReplaySession(
      fromSeqNr: Long,
      filterAfterSeqNr: Long,
      queue: SinkQueueWithCancel[EventEnvelope[Any]])

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FilterStage(
    streamId: String,
    entityType: String,
    sliceRange: Range,
    var initFilter: Iterable[FilterCriteria],
    currentEventsByPersistenceId: (String, Long) => Source[EventEnvelope[Any], NotUsed],
    val producerFilter: EventEnvelope[Any] => Boolean,
    replicatedEventOriginFilter: EventEnvelope[_] => Boolean,
    topicTagPrefix: String,
    replayParallelism: Int)
    extends GraphStage[BidiShape[StreamIn, NotUsed, EventEnvelope[Any], EventEnvelope[Any]]] {
  import ProtobufProtocolConversions._

  private val log = LoggerFactory.getLogger(classOf[FilterStage])

  import FilterStage._
  private val inReq = Inlet[StreamIn]("in1")
  private val inEnv = Inlet[EventEnvelope[Any]]("in2")
  private val outNotUsed = Outlet[NotUsed]("out1")
  private val outEnv = Outlet[EventEnvelope[Any]]("out2")
  override val shape = BidiShape(inReq, outNotUsed, inEnv, outEnv)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var persistence: Persistence = _

      // only one pull replay stream -> async callback at a time
      private var replayHasBeenPulled = false
      private var pendingReplayRequests: Vector[ReplayPersistenceId] = Vector.empty
      // several replay streams may be in progress at the same time
      private var replayInProgress: Map[String, ReplaySession] = Map.empty
      private val replayCallback = getAsyncCallback[Try[ReplayEnvelope]] {
        case Success(replayEnv) => onReplay(replayEnv)
        case Failure(exc)       => failStage(exc)
      }

      private var replicaId: Option[ReplicaId] = None

      private val logPrefix = s"$streamId (${sliceRange.min}-${sliceRange.max})"

      override def preStart(): Unit = {
        persistence = Persistence(materializer.system)
        updateFilter(initFilter)
        replayFromFilterCriteria(initFilter)
        initFilter = Nil // for GC
      }

      private def onReplay(replayEnv: ReplayEnvelope): Unit = {
        def replayCompleted(): Unit = {
          replayInProgress -= replayEnv.persistenceId
          pullInEnvOrReplay()
        }

        val currentReplay = replayInProgress(replayEnv.persistenceId)
        replayHasBeenPulled = false

        replayEnv match {
          case ReplayEnvelope(_, Some(env)) =>
            // the predicate to replay events from start for a given pid

            // protobuf ReplayReq was changed in akka-projection-grpc version 1.5.3 in a compatible way.
            // For consumers running an earlier version the filterAfterSeqNr will be 0 (default value in protobuf)
            val filterAfterSeqNr =
              if (currentReplay.filterAfterSeqNr == 0) Long.MaxValue else currentReplay.filterAfterSeqNr

            // Note: we do not apply the filter before filterAfterSeqNr as that may be what triggered the replay.
            // replicatedEventOriginFilter not used for replay.
            if (env.sequenceNr < filterAfterSeqNr || producerFilter(env) && filter.matches(env)) {
              log.trace(
                "Stream [{}]: Push replayed event persistenceId [{}], seqNr [{}]",
                logPrefix,
                env.persistenceId,
                env.sequenceNr)
              push(outEnv, env)
            } else {
              log.debug(
                "Stream [{}]: Filter out replayed event persistenceId [{}], seqNr [{}]",
                logPrefix,
                env.persistenceId,
                env.sequenceNr)
              pullInEnvOrReplay()
            }

          case ReplayEnvelope(persistenceId, None) =>
            log.debug("Stream [{}]: Completed replay of persistenceId [{}]", logPrefix, persistenceId)
            replayCompleted()
        }
      }

      private def tryPullReplay(pid: String): Unit = {
        if (!replayHasBeenPulled && isAvailable(outEnv) && !hasBeenPulled(inEnv)) {
          log.trace("Stream [{}]: tryPullReplay persistenceId [{}]", logPrefix, pid)
          val next =
            replayInProgress(pid).queue.pull().map(ReplayEnvelope(pid, _))(ExecutionContext.parasitic)
          next.value match {
            case None =>
              replayHasBeenPulled = true
              next.onComplete(replayCallback.invoke)(ExecutionContext.parasitic)
            case Some(Success(replayEnv)) =>
              onReplay(replayEnv)
            case Some(Failure(exc)) =>
              failStage(exc)
          }
        }
      }

      private var filter = Filter.empty(topicTagPrefix)

      private def updateFilter(criteria: Iterable[FilterCriteria]): Unit = {
        filter = updateFilterFromProto(filter, criteria, mapEntityIdToPidHandledByThisStream)
        log.trace("Stream [{}]: updated filter to [{}}]", logPrefix, filter)
      }

      private def replicaIdHandledByThisStream(pid: String): Boolean = {
        replicaId match {
          case None     => true
          case Some(id) => ReplicationId.fromString(pid).replicaId == id
        }
      }

      private def sliceHandledByThisStream(pid: String): Boolean = {
        val slice = persistence.sliceForPersistenceId(pid)
        sliceRange.contains(slice)
      }

      // Translate the streamId to the entityType and thereby constructing the full persistenceId.
      private def mapEntityIdToPid(entityId: String): String = {
        if (entityId.indexOf(ReplicationIdSeparator) < 0)
          PersistenceId(entityType, entityId).id
        else
          ReplicationId.fromString(s"$streamId$ReplicationIdSeparator$entityId").persistenceId.id
      }

      // Translate the streamId to the entityType and thereby constructing the full persistenceId.
      private def mapEntityIdToPidHandledByThisStream(entityIds: Seq[String]): Seq[String] =
        entityIds
          .map(mapEntityIdToPid)
          .filter(sliceHandledByThisStream)

      // Translate the streamId to the entityType and thereby constructing the full persistenceId.
      private def mapEntityIdOffsetToPidHandledByThisStream(
          entityIdOffsets: Seq[EntityIdOffset]): Seq[PersistenceIdSeqNr] =
        entityIdOffsets.flatMap { offset =>
          val pid = mapEntityIdToPid(offset.entityId)
          if (sliceHandledByThisStream(pid)) Some(PersistenceIdSeqNr(pid, offset.seqNr))
          else None
        }

      private def replayFromFilterCriteria(criteria: Iterable[FilterCriteria]): Unit = {
        criteria.foreach {
          _.message match {
            case FilterCriteria.Message.IncludeEntityIds(include) =>
              val replayPersistenceIds = mapEntityIdOffsetToPidHandledByThisStream(include.entityIdOffset)
                .map(p => ReplayPersistenceId(Some(p), filterAfterSeqNr = Long.MaxValue))
              replayAll(replayPersistenceIds)
            case _ =>
          }
        }
      }

      private def replayAll(replayPersistenceIds: Iterable[ReplayPersistenceId]): Unit = {
        replayPersistenceIds.foreach {
          case r @ ReplayPersistenceId(Some(fromOffset), _, _) if fromOffset.seqNr >= 1 =>
            replay(r)
          case _ =>
          // FIXME seqNr 0 would be to support a mode where we only deliver events after the include filter
          // change. In that case we must have a way to signal to the R2dbcOffsetStore that the
          // first seqNr of that new pid is ok to pass through even though it isn't 1
          // and the offset store doesn't know about previous seqNr.
        }
      }

      private def replay(replayPersistenceId: ReplayPersistenceId): Unit = {
        val persistenceIdOffset = replayPersistenceId.fromPersistenceIdOffset.get
        val fromSeqNr = persistenceIdOffset.seqNr
        val pid = persistenceIdOffset.persistenceId
        if (replicaIdHandledByThisStream(pid) && sliceHandledByThisStream(pid)) {
          val sameInProgress =
            replayInProgress.get(pid) match {
              case Some(replay) if replay.fromSeqNr == fromSeqNr =>
                // no point in cancel and starting new from same seqNr
                true
              case Some(replay) =>
                log.debug("Stream [{}]: Cancel replay of persistenceId [{}], replaced by new replay", logPrefix, pid)
                replay.queue.cancel()
                replayInProgress -= pid
                false
              case None =>
                false
            }

          if (sameInProgress) {
            log.debug(
              "Stream [{}]: Replay of persistenceId [{}] already in progress from seqNr [{}], replaced by new replay",
              logPrefix,
              pid,
              fromSeqNr)
            replayInProgress = replayInProgress.updated(
              pid,
              replayInProgress(pid).copy(filterAfterSeqNr = replayPersistenceId.filterAfterSeqNr))
          } else if (replayInProgress.size < replayParallelism) {
            log.debug("Stream [{}]: Starting replay of persistenceId [{}], from seqNr [{}]", logPrefix, pid, fromSeqNr)
            val queue =
              currentEventsByPersistenceId(pid, fromSeqNr)
                .runWith(Sink.queue())(materializer)
            replayInProgress =
              replayInProgress.updated(pid, ReplaySession(fromSeqNr, replayPersistenceId.filterAfterSeqNr, queue))
            tryPullReplay(pid)
          } else {
            log.debug("Stream [{}]: Queueing replay of persistenceId [{}], from seqNr [{}]", logPrefix, pid, fromSeqNr)
            pendingReplayRequests =
              pendingReplayRequests.filterNot(_.fromPersistenceIdOffset.get.persistenceId == pid) :+ replayPersistenceId
          }
        }
      }

      private def pullInEnvOrReplay(): Unit = {
        if (replayInProgress.size < replayParallelism && pendingReplayRequests.nonEmpty) {
          val pendingReplay = pendingReplayRequests.head
          pendingReplayRequests = pendingReplayRequests.tail
          replay(pendingReplay)
        }

        if (replayInProgress.isEmpty) {
          log.trace("Stream [{}]: Pull inEnv", logPrefix)
          pull(inEnv)
        } else {
          tryPullReplay(replayInProgress.head._1)
        }
      }

      setHandler(
        inReq,
        new InHandler {
          override def onPush(): Unit = {
            grab(inReq) match {
              case StreamIn(StreamIn.Message.Filter(filterReq), _) =>
                log.debug("Stream [{}]: Filter update requested [{}]", logPrefix, filterReq.criteria)
                updateFilter(filterReq.criteria)
                replayFromFilterCriteria(filterReq.criteria)

              case StreamIn(StreamIn.Message.Replay(replayReq), _) =>
                if (replayReq.replayPersistenceIds.nonEmpty) {
                  log.debug("Stream [{}]: Replay requested for [{}]", logPrefix, replayReq.replayPersistenceIds)
                  replayAll(replayReq.replayPersistenceIds)
                }
                // needed for compatibility with 2.5.2
                if (replayReq.replayPersistenceIds.isEmpty && replayReq.persistenceIdOffset.nonEmpty) {
                  log.debug("Stream [{}]: Replay requested for [{}]", logPrefix, replayReq.persistenceIdOffset)
                  replayAll(replayReq.persistenceIdOffset.map(p =>
                    ReplayPersistenceId(Some(p), filterAfterSeqNr = Long.MaxValue)))
                }

              case StreamIn(StreamIn.Message.Init(_), _) =>
                log.warn("Stream [{}]: Init request can only be used as the first message", logPrefix)
                throw new IllegalStateException("Init request can only be used as the first message")

              case StreamIn(other, _) =>
                log.warn("Stream [{}]: Unknown StreamIn request [{}]", logPrefix, other.getClass.getName)
            }

            pull(inReq)
          }
        })

      setHandler(
        inEnv,
        new InHandler {
          override def onPush(): Unit = {
            val env = grab(inEnv)
            val pid = env.persistenceId

            // replicaId is used for validation of replay requests, to avoid replay for other replicas
            if (replicaId.isEmpty && env.metadata[ReplicatedEventMetadata].isDefined)
              replicaId = Some(ReplicationId.fromString(pid).replicaId)

            if (producerFilter(env) && filter.matches(env)) {
              if (replicatedEventOriginFilter(env)) {
                // Note that the producer filter has higher priority - if a producer decides to filter events out the consumer
                // can never include them
                log.trace("Stream [{}]: Push event persistenceId [{}], seqNr [{}]", logPrefix, pid, env.sequenceNr)
                push(outEnv, env)
              } else {
                log.trace(
                  "Stream [{}]: Filter event, due to origin, persistenceId [{}], seqNr [{}]",
                  logPrefix,
                  pid,
                  env.sequenceNr)
                push(outEnv, env.withEvent(FilteredPayload)) // FilteredPayload will be transformed to FilteredEvent
              }
            } else {
              log.debug("Stream [{}]: Filter out event persistenceId [{}], seqNr [{}]", logPrefix, pid, env.sequenceNr)
              pullInEnvOrReplay()
            }
          }
        })

      setHandler(outNotUsed, new OutHandler {
        override def onPull(): Unit = {
          pull(inReq)
        }
      })

      setHandler(outEnv, new OutHandler {
        override def onPull(): Unit = {
          log.trace("Stream [{}]: onPull outEnv", logPrefix)
          pullInEnvOrReplay()
        }
      })
    }

}
