/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import akka.NotUsed
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.dispatch.ExecutionContexts
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.query.typed.scaladsl.CurrentEventsByPersistenceIdTypedQuery
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicationId
import akka.projection.grpc.internal.proto.EntityIdOffset
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.PersistenceIdSeqNr
import akka.projection.grpc.internal.proto.StreamIn
import akka.stream.Attributes
import akka.stream.BidiShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.SinkQueueWithCancel
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import org.slf4j.LoggerFactory

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FilterStage {
  val ReplayParallelism = 3 // FIXME config
  private val ReplicationIdSeparator = '|'

  object Filter {
    val empty: Filter = Filter(Set.empty, Set.empty, Map.empty)
  }

  final case class Filter(
      includePersistenceIds: Set[String],
      excludePersistenceIds: Set[String],
      excludeRegexEntityIds: Map[String, Regex]) {

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

    /**
     * Exclude criteria are evaluated first.
     * Returns `true` if no matching exclude.
     * If an exclude is matching the include criteria are evaluated.
     * Returns `true` if there is a matching include, otherwise `false`.
     */
    def matches(pid: String): Boolean = {
      val entityId = PersistenceId.extractEntityId(pid)

      if (excludePersistenceIds.contains(pid) || excludeRegexEntityIds.exists {
            case (_, regex) =>
              regex.pattern.matcher(entityId).matches()
          }) {
        includePersistenceIds.contains(pid)
      } else {
        true
      }
    }
  }

  private case class ReplayEnvelope(persistenceId: String, env: Option[EventEnvelope[Any]])

  private case class ReplaySession(fromSeqNr: Long, queue: SinkQueueWithCancel[EventEnvelope[Any]])

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FilterStage(
    streamId: String,
    entityType: String,
    sliceRange: Range,
    var initFilter: Iterable[FilterCriteria],
    currentEventsByPersistenceIdQuery: CurrentEventsByPersistenceIdTypedQuery,
    val producerFilter: EventEnvelope[Any] => Boolean)
    extends GraphStage[BidiShape[StreamIn, NotUsed, EventEnvelope[Any], EventEnvelope[Any]]] {

  private val log = LoggerFactory.getLogger(classOf[FilterStage])

  import FilterStage._
  private val inReq = Inlet[StreamIn]("in1")
  private val inEnv = Inlet[EventEnvelope[Any]]("in2")
  private val outNotUsed = Outlet[NotUsed]("out1")
  // FIXME probably need something else than EventEnvelope out later if need to filter on individual events to pass on pid/seqNr
  private val outEnv = Outlet[EventEnvelope[Any]]("out2")
  override val shape = BidiShape(inReq, outNotUsed, inEnv, outEnv)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {

      private var persistence: Persistence = _

      // only one pull replay stream -> async callback at a time
      private var replayHasBeenPulled = false
      private var pendingReplayRequests: Vector[PersistenceIdSeqNr] = Vector.empty
      // several replay streams may be in progress at the same time
      private var replayInProgress: Map[String, ReplaySession] = Map.empty
      private val replayCallback = getAsyncCallback[Try[ReplayEnvelope]] {
        case Success(replayEnv) => onReplay(replayEnv)
        case Failure(exc)       => failStage(exc)
      }

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

        replayHasBeenPulled = false

        replayEnv match {
          case ReplayEnvelope(persistenceId, Some(env)) =>
            // the predicate to replay events from start for a given pid
            // Note: we do not apply the producer filter here as that may be what triggered the replay
            if (filter.matches(env.persistenceId)) {
              log.traceN(
                "Stream [{}]: Push replayed event persistenceId [{}], seqNr [{}]",
                logPrefix,
                env.persistenceId,
                env.sequenceNr)
              push(outEnv, env)
            } else {
              log.debugN(
                "Stream [{}]: Filter out replayed event persistenceId [{}], seqNr [{}]. Cancel remaining replay.",
                logPrefix,
                env.persistenceId,
                env.sequenceNr)
              val queue = replayInProgress(persistenceId).queue
              queue.cancel()
              replayCompleted()
            }

          case ReplayEnvelope(persistenceId, None) =>
            log.debug2("Stream [{}]: Completed replay of persistenceId [{}]", logPrefix, persistenceId)
            replayCompleted()
        }
      }

      private def tryPullReplay(pid: String): Unit = {
        if (!replayHasBeenPulled && isAvailable(outEnv) && !hasBeenPulled(inEnv)) {
          log.trace2("Stream [{}]: tryPullReplay persistenceId [{}}]", logPrefix, pid)
          val next =
            replayInProgress(pid).queue.pull().map(ReplayEnvelope(pid, _))(ExecutionContexts.parasitic)
          next.value match {
            case None =>
              replayHasBeenPulled = true
              next.onComplete(replayCallback.invoke)(ExecutionContexts.parasitic)
            case Some(Success(replayEnv)) =>
              onReplay(replayEnv)
            case Some(Failure(exc)) =>
              failStage(exc)
          }
        }
      }

      private var filter = Filter.empty

      private def updateFilter(criteria: Iterable[FilterCriteria]): Unit = {
        filter = criteria.foldLeft(filter) {
          case (acc, criteria) =>
            criteria.message match {
              case FilterCriteria.Message.IncludeEntityIds(include) =>
                val pids = mapEntityIdToPidHandledByThisStream(include.entityIdOffset.map(_.entityId))
                acc.addIncludePersistenceIds(pids)
              case FilterCriteria.Message.RemoveIncludeEntityIds(include) =>
                val pids = mapEntityIdToPidHandledByThisStream(include.entityIds)
                acc.removeIncludePersistenceIds(pids)
              case FilterCriteria.Message.ExcludeEntityIds(exclude) =>
                val pids = mapEntityIdToPidHandledByThisStream(exclude.entityIds)
                acc.addExcludePersistenceIds(pids)
              case FilterCriteria.Message.RemoveExcludeEntityIds(exclude) =>
                val pids = mapEntityIdToPidHandledByThisStream(exclude.entityIds)
                acc.removeExcludePersistenceIds(pids)
              case FilterCriteria.Message.ExcludeMatchingEntityIds(excludeRegex) =>
                acc.addExcludeRegexEntityIds(excludeRegex.matching)
              case FilterCriteria.Message.RemoveExcludeMatchingEntityIds(excludeRegex) =>
                acc.removeExcludeRegexEntityIds(excludeRegex.matching)
              case FilterCriteria.Message.Empty =>
                acc
            }
        }
        log.trace2("Stream [{}]: updated filter to [{}}]", logPrefix, filter)
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
              replayAll(mapEntityIdOffsetToPidHandledByThisStream(include.entityIdOffset))
            case _ =>
          }
        }
      }

      private def replayAll(persistenceIdOffsets: Iterable[PersistenceIdSeqNr]): Unit = {
        // FIXME limit number of concurrent replay requests, place additional in a pending queue
        persistenceIdOffsets.foreach { offset =>
          if (offset.seqNr >= 1)
            replay(offset)
        // FIXME seqNr 0 would be to support a mode where we only deliver events after the include filter
        // change. In that case we must have a way to signal to the R2dbcOffsetStore that the
        // first seqNr of that new pid is ok to pass through even though it isn't 1
        // and the offset store doesn't know about previous seqNr.
        }
      }

      private def replay(persistenceIdOffset: PersistenceIdSeqNr): Unit = {
        val fromSeqNr = persistenceIdOffset.seqNr
        // FIXME what about the replicaId when using RES?
        val pid = persistenceIdOffset.persistenceId
        if (sliceHandledByThisStream(pid)) {
          val sameInProgress =
            replayInProgress.get(pid) match {
              case Some(replay) if replay.fromSeqNr == fromSeqNr =>
                // no point in cancel and starting new from same seqNr
                true
              case Some(replay) =>
                log.debug2("Stream [{}]: Cancel replay of persistenceId [{}], replaced by new replay", logPrefix, pid)
                replay.queue.cancel()
                replayInProgress -= pid
                false
              case None =>
                false
            }

          if (sameInProgress) {
            log.debugN(
              "Stream [{}]: Replay of persistenceId [{}] already in progress from seqNr [{}], replaced by new replay",
              logPrefix,
              pid,
              fromSeqNr)
          } else if (replayInProgress.size < ReplayParallelism) {
            log.debugN("Stream [{}]: Starting replay of persistenceId [{}], from seqNr [{}]", logPrefix, pid, fromSeqNr)
            val queue =
              currentEventsByPersistenceIdQuery
                .currentEventsByPersistenceIdTyped[Any](pid, fromSeqNr, Long.MaxValue)
                .runWith(Sink.queue())(materializer)
            replayInProgress = replayInProgress.updated(pid, ReplaySession(fromSeqNr, queue))
            tryPullReplay(pid)
          } else {
            log.debugN("Stream [{}]: Queueing replay of persistenceId [{}], from seqNr [{}]", logPrefix, pid, fromSeqNr)
            pendingReplayRequests = pendingReplayRequests.filterNot(_.persistenceId == pid) :+ persistenceIdOffset
          }
        }
      }

      private def pullInEnvOrReplay(): Unit = {
        if (replayInProgress.size < ReplayParallelism && pendingReplayRequests.nonEmpty) {
          val pendingEntityOffset = pendingReplayRequests.head
          pendingReplayRequests = pendingReplayRequests.tail
          replay(pendingEntityOffset)
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
                log.debug2("Stream [{}]: Filter update requested [{}]", logPrefix, filterReq.criteria)
                updateFilter(filterReq.criteria)
                replayFromFilterCriteria(filterReq.criteria)

              case StreamIn(StreamIn.Message.Replay(replayReq), _) =>
                if (replayReq.persistenceIdOffset.nonEmpty) {
                  log.debug2("Stream [{}]: Replay requested for [{}]", logPrefix, replayReq.persistenceIdOffset)
                  // FIXME for RES, can we check if the replicaId is handled by this stream, to avoid unnecessary replay queries
                  replayAll(replayReq.persistenceIdOffset)
                }

              case StreamIn(StreamIn.Message.Init(_), _) =>
                log.warn("Stream [{}]: Init request can only be used as the first message", logPrefix)
                throw new IllegalStateException("Init request can only be used as the first message")

              case StreamIn(other, _) =>
                log.warn2("Stream [{}]: Unknown StreamIn request [{}]", logPrefix, other.getClass.getName)
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

            // Note that the producer filter has higher priority - if a producer decides to filter events out the consumer
            // can never include them
            if (producerFilter(env) && filter.matches(pid)) {
              log.traceN("Stream [{}]: Push event persistenceId [{}], seqNr [{}]", logPrefix, pid, env.sequenceNr)
              push(outEnv, env)
            } else {
              log.debugN("Stream [{}]: Filter out event persistenceId [{}], seqNr [{}]", logPrefix, pid, env.sequenceNr)
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
