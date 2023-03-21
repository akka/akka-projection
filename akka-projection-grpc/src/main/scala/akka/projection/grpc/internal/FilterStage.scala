/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import java.time.Instant

import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.util.matching.Regex

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.TimestampOffset
import akka.persistence.query.scaladsl.CurrentEventsByPersistenceIdQuery
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilterReq
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
import akka.stream.stage.StageLogging

/**
 * INTERNAL API
 */
@InternalApi private[akka] object FilterStage {
  object Filter {
    val empty: Filter = Filter(Set.empty, Set.empty, Vector.empty)
  }

  final case class Filter(
      includePersistenceIds: Set[String],
      excludePersistenceIds: Set[String],
      excludeRegexEntityIds: Vector[Regex]) {

    def addIncludePersistenceIds(pids: Iterable[String]): Filter =
      copy(includePersistenceIds = includePersistenceIds ++ pids)

    def addExcludePersistenceIds(pids: Iterable[String]): Filter =
      copy(excludePersistenceIds = excludePersistenceIds ++ pids)

    def addExcludeRegexEntityIds(reqex: Iterable[Regex]): Filter =
      copy(excludeRegexEntityIds = excludeRegexEntityIds ++ reqex)

    /**
     * Exclude criteria are evaluated first.
     * Returns `true` if no matching exclude.
     * If an exclude is matching the include criteria are evaluated.
     * Returns `true` if there is a matching include, otherwise `false`.
     */
    def matches(pid: String): Boolean = {
      val entityId = PersistenceId.extractEntityId(pid)

      if (excludePersistenceIds.contains(pid) || excludeRegexEntityIds.exists { regex =>
            regex.matches(entityId)
          }) {
        includePersistenceIds.contains(pid)
      } else {
        true
      }
    }
  }

  private case class ReplayEnvelope(entityId: String, env: Option[EventEnvelope[Any]])

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FilterStage(
    streamId: String,
    entityType: String,
    sliceRange: Range,
    var initFilter: Iterable[FilterCriteria],
    currentEventsByPersistenceIdQuery: CurrentEventsByPersistenceIdQuery,
    verbose: Boolean = false)
    extends GraphStage[BidiShape[StreamIn, NotUsed, EventEnvelope[Any], EventEnvelope[Any]]] {
  import FilterStage._
  private val inReq = Inlet[StreamIn]("in1")
  private val inEnv = Inlet[EventEnvelope[Any]]("in2")
  private val outNotUsed = Outlet[NotUsed]("out1")
  // FIXME probably need something else than EventEnvelope out later if need to filter on individual events to pass on pid/seqNr
  private val outEnv = Outlet[EventEnvelope[Any]]("out2")
  override val shape = BidiShape(inReq, outNotUsed, inEnv, outEnv)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      private var persistence: Persistence = _

      // only one pull replay stream -> async callback at a time
      private var replayHasBeenPulled = false
      // several replay streams may be in progress at the same time
      private var replayInProgress: Map[String, SinkQueueWithCancel[EventEnvelope[Any]]] = Map.empty
      private val replayCallback = getAsyncCallback[Try[ReplayEnvelope]] {
        case Success(replayEnv) => onReplay(replayEnv)
        case Failure(exc)       => failStage(exc)
      }

      private val logPrefix = s"$streamId (${sliceRange.min}-${sliceRange.max})"

      override def preStart(): Unit = {
        persistence = Persistence(materializer.system)
        updateFilter(initFilter)
        replay(initFilter)
        initFilter = Nil // for GC
      }

      override protected def logSource: Class[_] = classOf[FilterStage]

      private def onReplay(replayEnv: ReplayEnvelope): Unit = {
        def replayCompleted(): Unit = {
          replayInProgress -= replayEnv.entityId
          pullInEnvOrReplay()
        }

        replayHasBeenPulled = false

        replayEnv match {
          case ReplayEnvelope(entityId, Some(env)) =>
            if (filter.matches(env.persistenceId)) {
              if (verbose)
                log.debug(
                  "Stream [{}]: Push replayed event persistenceId [{}], seqNr [{}]",
                  logPrefix,
                  env.persistenceId,
                  env.sequenceNr)
              push(outEnv, env)
            } else {
              log.debug(
                "Stream [{}]: Filter out replayed event persistenceId [{}], seqNr [{}]. Cancel remaining replay.",
                logPrefix,
                env.persistenceId,
                env.sequenceNr)
              val queue = replayInProgress(entityId)
              queue.cancel()
              replayCompleted()
            }

          case ReplayEnvelope(entityId, None) =>
            log.debug("Stream [{}]: Completed replay of entityId [{}]", logPrefix, entityId)
            replayCompleted()
        }
      }

      private def tryPullReplay(entityId: String): Unit = {
        if (!replayHasBeenPulled && isAvailable(outEnv) && !hasBeenPulled(inEnv)) {
          replayHasBeenPulled = true
          if (verbose)
            log.debug("Stream [{}]: tryPullReplay entityId [{}}]", logPrefix, entityId)
          implicit val ec: ExecutionContext = materializer.executionContext
          val next = replayInProgress(entityId).pull().map(ReplayEnvelope(entityId, _))
          next.onComplete(replayCallback.invoke)
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
              case FilterCriteria.Message.ExcludeEntityIds(exclude) =>
                val pids = mapEntityIdToPidHandledByThisStream(exclude.entityIds)
                acc.addExcludePersistenceIds(pids)

              case FilterCriteria.Message.ExcludeMatchingEntityIds(excludeRegex) =>
                // FIXME add the entityType prefix to the regex so that it is matching the persistenceId ?
                val regex = excludeRegex.matching.map(_.r)
                acc.addExcludeRegexEntityIds(regex)
              case FilterCriteria.Message.Empty =>
                acc
            }
        }
      }

      private def handledByThisStream(pid: PersistenceId): Boolean = {
        // note that it's not possible to decide this on the consumer side because there we don't know the
        // mapping between streamId and entityType
        val slice = persistence.sliceForPersistenceId(pid.id)
        sliceRange.contains(slice)
      }

      // Translate the streamId to the entityType and thereby constructing the full persistenceId.
      private def mapEntityIdToPidHandledByThisStream(entityIds: Seq[String]): Seq[String] =
        entityIds.map(PersistenceId(entityType, _)).filter(handledByThisStream).map(_.id)

      private def replay(criteria: Iterable[FilterCriteria]): Unit = {
        // FIXME limit number of concurrent replay requests, place additional in a pending queue
        criteria.foreach {
          _.message match {
            case FilterCriteria.Message.IncludeEntityIds(include) =>
              include.entityIdOffset.foreach { entityOffset =>
                if (entityOffset.seqNr >= 1)
                  replay(entityOffset.entityId, entityOffset.seqNr)
              // FIXME seqNr 0 would be to support a mode where we only deliver events after the include filter
              // change. In that case we must have a way to signal to the R2dbcOffsetStore that the
              // first seqNr of that new pid is ok to pass through even though it isn't 1
              // and the offset store doesn't know about previous seqNr.
              }
            case _ =>
          }
        }
      }

      private def replay(entityId: String, fromSeqNr: Long): Unit = {
        val pid = PersistenceId(entityType, entityId)
        if (handledByThisStream(pid)) {
          replayInProgress.get(entityId).foreach { queue =>
            log.debug("Stream [{}]: Cancel replay of entityId [{}], replaced by new replay", logPrefix, entityId)
            queue.cancel()
          }

          log.debug("Stream [{}]: Starting replay of entityId [{}], from seqNr [{}]", logPrefix, entityId, fromSeqNr)
          val slice = persistence.sliceForPersistenceId(pid.id)
          val queue =
            currentEventsByPersistenceIdQuery
              .currentEventsByPersistenceId(pid.id, fromSeqNr, Long.MaxValue)
              .map { env =>
                // FIXME "wrong" timestamp, we should probably define a new currentEventsByPersistenceId that matches eventsBySlices
                val offset =
                  TimestampOffset(Instant.ofEpochMilli(env.timestamp), Map(env.persistenceId -> env.sequenceNr))
                EventEnvelope(offset, env.persistenceId, env.sequenceNr, env.event, env.timestamp, entityType, slice)
              }
              .runWith(Sink.queue())(materializer)
          replayInProgress = replayInProgress.updated(entityId, queue)
          tryPullReplay(entityId)
        }
      }

      private def pullInEnvOrReplay(): Unit = {
        if (replayInProgress.isEmpty) {
          if (verbose)
            log.debug("Stream [{}]: Pull inEnv", logPrefix)
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
              case StreamIn(StreamIn.Message.Filter(filterReq: FilterReq), _) =>
                updateFilter(filterReq.criteria)
                replay(filterReq.criteria)

              case StreamIn(other, _) =>
                log.warning("Stream [{}]: Unknown StreamIn request [{}]", logPrefix, other.getClass.getName)
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

            if (filter.matches(pid)) {
              if (verbose)
                log.debug("Stream [{}]: Push event persistenceId [{}], seqNr [{}]", logPrefix, pid, env.sequenceNr)
              push(outEnv, env)
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
          if (verbose)
            log.debug("Stream [{}]: onPull outEnv", logPrefix)
          pullInEnvOrReplay()
        }
      })
    }

}
