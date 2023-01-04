/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.Done
import akka.dispatch.ExecutionContexts
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[akka] object ParallelUpdatesFlow {
  final class Holder[T](val element: (EventEnvelope[T], ProjectionContext), var completed: Boolean) {
    def persistenceId: String = element._1.persistenceId
    def envelope: EventEnvelope[T] = element._1
  }
}

// Simpler version of MapAsyncPartitioned until that is ready, better than just mapAsync(1) but second element
// for the same persistence id will block pulling elements for other pids until the original one has completed
// FIXME replace with MapAsyncPartitioned once available
private[akka] final class ParallelUpdatesFlow[T](parallelism: Int)(f: EventEnvelope[T] => Future[Done])
    extends GraphStage[FlowShape[(EventEnvelope[T], ProjectionContext), (EventEnvelope[T], ProjectionContext)]] {
  import ParallelUpdatesFlow._

  val in = Inlet[(EventEnvelope[T], ProjectionContext)]("in")
  val out = Outlet[(EventEnvelope[T], ProjectionContext)]("out")
  val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    private var blockedByInFlight: Option[Holder[T]] = None
    private val inFlight = mutable.Queue[Holder[T]]()

    private val onCompleteCallback = getAsyncCallback(onComplete).invoke(_)

    private def onComplete(result: Try[String]): Unit = result match {
      case Success(persistenceId) =>
        inFlight.find(_.persistenceId == persistenceId).get.completed = true
        emitHeadIfPossible()

      case Failure(ex) => throw ex
    }

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val holder = new Holder[T](grab(in), false)
          if (inFlight.exists(_.persistenceId == holder.persistenceId)) {
            blockedByInFlight = Some(holder)
          } else {
            inFlight.enqueue(holder)
            processElement(holder)
            pullNextIfPossible()
          }
        }

        override def onUpstreamFinish(): Unit = {
          if (inFlight.isEmpty) completeStage()
          // else keep going and complete once queue is empty
        }
      })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        if (inFlight.nonEmpty && inFlight.head.completed) {
          emitHeadIfPossible()
        } else {
          pullNextIfPossible()
        }
      }
    })

    private def processElement(holder: Holder[T]): Unit = {
      f(holder.envelope)
        .map(_ => holder.persistenceId)(ExecutionContexts.parasitic)
        .onComplete(onCompleteCallback)(ExecutionContexts.parasitic)
    }

    private def emitHeadIfPossible(): Unit = {
      if (inFlight.head.completed && isAvailable(out)) {
        val head = inFlight.dequeue()
        push(out, head.element)
        blockedByInFlight match {
          case Some(blocked) =>
            if (blocked.persistenceId == head.persistenceId) {
              // we're now unblocked
              blockedByInFlight = None
              processElement(blocked)
              inFlight.enqueue(blocked)
              pullNextIfPossible()
            }
          case None =>
            pullNextIfPossible()
        }
        if (isClosed(in) && inFlight.isEmpty) completeStage()
      }
    }

    private def pullNextIfPossible(): Unit = {
      if (blockedByInFlight.isEmpty && inFlight.size < parallelism && !isClosed(in) && !hasBeenPulled(in)) {
        pull(in)
      }
    }
  }
}
