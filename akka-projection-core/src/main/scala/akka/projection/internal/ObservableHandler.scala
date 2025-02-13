/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.Done
import akka.NotUsed
import akka.annotation.InternalApi
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.StatusObserver
import akka.projection.scaladsl.Handler
import akka.stream.scaladsl.Flow

/** INTERNAL API */
@InternalApi
private[projection] trait HandlerObserver[-Envelope] {
  def beforeProcess(envelope: Envelope): AnyRef
  def afterProcess(envelope: Envelope, context: AnyRef): Unit
}

/** INTERNAL API */
@InternalApi
private[projection] object NoopHandlerObserver extends HandlerObserver[Any] {
  def beforeProcess(envelope: Any): AnyRef = null
  def afterProcess(envelope: Any, context: AnyRef): Unit = ()
}

/** INTERNAL API */
@InternalApi
private[projection] final class SingleHandlerObserver[Envelope](
    projectionId: ProjectionId,
    statusObserver: StatusObserver[Envelope],
    telemetry: Telemetry,
    extractCreationTime: Envelope => Long)
    extends HandlerObserver[Envelope] {

  override def beforeProcess(envelope: Envelope): AnyRef = {
    statusObserver.beforeProcess(projectionId, envelope)
    telemetry.beforeProcess(envelope, extractCreationTime(envelope))
  }

  override def afterProcess(envelope: Envelope, context: AnyRef): Unit = {
    statusObserver.afterProcess(projectionId, envelope)
    telemetry.afterProcess(context)
  }
}

/** INTERNAL API */
@InternalApi
private[projection] final case class GroupedContexts(contexts: Seq[AnyRef])

/** INTERNAL API */
@InternalApi
private[projection] final class GroupedHandlerObserver[Envelope](
    projectionId: ProjectionId,
    statusObserver: StatusObserver[Envelope],
    telemetry: Telemetry,
    extractCreationTime: Envelope => Long)
    extends HandlerObserver[Seq[Envelope]] {

  override def beforeProcess(envelopes: Seq[Envelope]): AnyRef = {
    val contexts = envelopes.map { envelope =>
      statusObserver.beforeProcess(projectionId, envelope)
      telemetry.beforeProcess(envelope, extractCreationTime(envelope))
    }
    GroupedContexts(contexts)
  }

  override def afterProcess(envelopes: Seq[Envelope], groupedContexts: AnyRef): Unit = {
    envelopes.foreach(envelope => statusObserver.afterProcess(projectionId, envelope))
    groupedContexts match {
      case GroupedContexts(contexts) => contexts.foreach(context => telemetry.afterProcess(context))
      case _                         =>
    }
  }
}

/** INTERNAL API */
@InternalApi
private[projection] object ObservableHandler {
  def observeProcess[Envelope, Result](
      observer: HandlerObserver[Envelope],
      envelope: Envelope,
      process: Envelope => Future[Result]): Future[Result] = {
    val context = observer.beforeProcess(envelope)
    process(envelope)
      .map { result =>
        observer.afterProcess(envelope, context)
        result
      }(ExecutionContext.parasitic)
  }
}

/** INTERNAL API */
@InternalApi
private[projection] final class ObservableHandler[Envelope](
    handler: Handler[Envelope],
    observer: HandlerObserver[Envelope])
    extends Handler[Envelope] {

  override def start(): Future[Done] = handler.start()

  override def stop(): Future[Done] = handler.stop()

  override def process(envelope: Envelope): Future[Done] = {
    ObservableHandler.observeProcess(observer, envelope, handler.process)
  }
}

/** INTERNAL API */
@InternalApi
private[projection] object ObservableFlowHandler {
  def apply[Offset, Envelope](
      flow: Flow[(Envelope, ProjectionContext), (Done, ProjectionContext), _],
      observer: HandlerObserver[Envelope])
      : Flow[ProjectionContextImpl[Offset, Envelope], ProjectionContextImpl[Offset, Envelope], NotUsed] = {
    Flow[ProjectionContextImpl[Offset, Envelope]]
      .map { projectionContext =>
        val envelope = projectionContext.envelope
        val context = observer.beforeProcess(envelope)
        envelope -> projectionContext.withObserver(observer).withExternalContext(context)
      }
      .via(flow)
      .map {
        case (_, context) =>
          val projectionContext = context.asInstanceOf[ProjectionContextImpl[Offset, Envelope]]
          observer.afterProcess(projectionContext.envelope, projectionContext.externalContext)
          projectionContext
      }
  }
}
