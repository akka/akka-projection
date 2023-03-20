/**
 * Copyright (C) 2022-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.internal

import akka.NotUsed
import akka.annotation.InternalApi
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.grpc.internal.proto.FilterEntityIdsReq
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
@InternalApi private[akka] class FilterStage(streamId: String, entityType: String, sliceRange: Range)
    extends GraphStage[BidiShape[StreamIn, NotUsed, EventEnvelope[Any], EventEnvelope[Any]]] {
  private val in1 = Inlet[StreamIn]("in1")
  private val in2 = Inlet[EventEnvelope[Any]]("in2")
  private val out1 = Outlet[NotUsed]("out1")
  // FIXME probably need something else than EventEnvelope out later if need to filter on individual events to pass on pid/seqNr
  private val out2 = Outlet[EventEnvelope[Any]]("out2")
  override val shape = BidiShape(in1, out1, in2, out2)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with StageLogging {

      private var persistence: Persistence = _

      private var includePersistenceIds = Set.empty[String]
      private var excludePersistenceIds = Set.empty[String]

      override def preStart(): Unit = {
        persistence = Persistence(materializer.system)
      }

      override protected def logSource: Class[_] = classOf[FilterStage]

      private def handledByThisStream(pid: PersistenceId): Boolean = {
        // note that it's not possible to decide this on the consumer side because there we don't know the
        // mapping between streamId and entityType
        val slice = persistence.sliceForPersistenceId(pid.id)
        sliceRange.contains(slice)
      }

      private def mapEntityIdToPidHandledByThisStream(entityIds: Seq[String]): Seq[String] =
        entityIds.map(PersistenceId(entityType, _)).filter(handledByThisStream).map(_.id)

      setHandler(
        in1,
        new InHandler {
          override def onPush(): Unit = {
            grab(in1) match {
              case StreamIn(
                  StreamIn.Message
                    .FilterEntityIds(filterReq: FilterEntityIdsReq),
                  _) =>
                // We translate the streamId to the entityType and thereby constructing the full persistenceId.
                val include = mapEntityIdToPidHandledByThisStream(filterReq.includeEntityIds)
                val exclude = mapEntityIdToPidHandledByThisStream(filterReq.excludeEntityIds)

                if (include.nonEmpty || exclude.nonEmpty) {
                  log.info(
                    "Stream [{}] change of filter include [{}], exclude [{}]",
                    streamId,
                    include.mkString(", "),
                    exclude.mkString(", "))
                  if (include.isEmpty && exclude.isEmpty) {
                    includePersistenceIds = Set.empty
                    excludePersistenceIds = Set.empty
                  } else {
                    // FIXME this logic is too fuzzy
                    includePersistenceIds ++= include
                    includePersistenceIds --= exclude
                    excludePersistenceIds ++= exclude
                    excludePersistenceIds --= include
                  }

                  // FIXME When adding new pid to includes we can load all events for that pid via
                  // eventsByPersistenceId and push them before pushing more from eventsBySlices (in2).
                  // We could also support a mode where we only deliver events after the include filter
                  // change. In that case we must have a way to signal to the R2dbcOffsetStore that the
                  // first seqNr of that new pid is ok to pass through even though it isn't 1
                  // and the offset store doesn't know about previous seqNr.

                }

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

            // FIXME this logic is too fuzzy
            val exclude =
              if (includePersistenceIds.isEmpty && excludePersistenceIds.isEmpty)
                false
              else if (includePersistenceIds(pid))
                false
              else if (!excludePersistenceIds(pid))
                false
              else
                true

            if (exclude) {
              log.debug("Stream [{}] Filter out event persistenceId [{}], seqNr [{}]", streamId, pid, env.sequenceNr)
              pull(in2)
            } else {
              push(out2, env)
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
