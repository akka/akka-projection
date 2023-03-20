/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.internal

import scala.util.matching.Regex

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.FilterCriteria
import akka.projection.grpc.internal.proto.FilterReq
import akka.projection.grpc.internal.proto.StreamIn
import akka.stream.Attributes
import akka.stream.BidiShape
import akka.stream.Inlet
import akka.stream.Outlet
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

}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class FilterStage(
    streamId: String,
    entityType: String,
    sliceRange: Range,
    var initFilter: Iterable[FilterCriteria])
    extends GraphStage[BidiShape[StreamIn, NotUsed, EventEnvelope[Any], EventEnvelope[Any]]] {
  import FilterStage._
  private val in1 = Inlet[StreamIn]("in1")
  private val in2 = Inlet[EventEnvelope[Any]]("in2")
  private val out1 = Outlet[NotUsed]("out1")
  // FIXME probably need something else than EventEnvelope out later if need to filter on individual events to pass on pid/seqNr
  private val out2 = Outlet[EventEnvelope[Any]]("out2")
  override val shape = BidiShape(in1, out1, in2, out2)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      private var persistence: Persistence = _

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

      override def preStart(): Unit = {
        persistence = Persistence(materializer.system)
        updateFilter(initFilter)
        initFilter = Nil // for GC
      }

      override protected def logSource: Class[_] = classOf[FilterStage]

      private def handledByThisStream(pid: PersistenceId): Boolean = {
        // note that it's not possible to decide this on the consumer side because there we don't know the
        // mapping between streamId and entityType
        val slice = persistence.sliceForPersistenceId(pid.id)
        sliceRange.contains(slice)
      }

      // Translate the streamId to the entityType and thereby constructing the full persistenceId.
      private def mapEntityIdToPidHandledByThisStream(entityIds: Seq[String]): Seq[String] =
        entityIds.map(PersistenceId(entityType, _)).filter(handledByThisStream).map(_.id)

      setHandler(
        in1,
        new InHandler {
          override def onPush(): Unit = {
            grab(in1) match {
              case StreamIn(StreamIn.Message.Filter(filterReq: FilterReq), _) =>
                updateFilter(filterReq.criteria)

              // FIXME When adding new pid to includes we can load all events for that pid via
              // eventsByPersistenceId and push them before pushing more from eventsBySlices (in2).
              // We could also support a mode where we only deliver events after the include filter
              // change. In that case we must have a way to signal to the R2dbcOffsetStore that the
              // first seqNr of that new pid is ok to pass through even though it isn't 1
              // and the offset store doesn't know about previous seqNr.

              case StreamIn(other, _) =>
                log.warning("Unknown StreamIn request [{}]", other.getClass.getName)
            }

            pull(in1)
          }
        })

      setHandler(
        in2,
        new InHandler {
          override def onPush(): Unit = {
            val env = grab(in2)
            val pid = env.persistenceId

            if (filter.matches(pid)) {
              push(out2, env)
            } else {
              log.debug("Stream [{}] Filter out event persistenceId [{}], seqNr [{}]", streamId, pid, env.sequenceNr)
              pull(in2)
            }
          }
        })

      setHandler(out1, new OutHandler {
        override def onPull(): Unit = {
          pull(in1)
        }
      })

      setHandler(out2, new OutHandler {
        override def onPull(): Unit = {
          pull(in2)
        }
      })
    }

}
