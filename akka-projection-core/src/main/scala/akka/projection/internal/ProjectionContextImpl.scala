/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.internal

import akka.annotation.InternalApi
import akka.projection.ProjectionContext

/**
 * INTERNAL API
 * @param groupSize is used only in GroupHandlerStrategies so a single context instance
 *                  can report that multiple envelopes were processed.
 */
@InternalApi private[projection] final case class ProjectionContextImpl[Offset, Envelope] private (
    offset: Offset,
    envelope: Envelope,
    observer: HandlerObserver[Envelope],
    externalContext: AnyRef,
    groupSize: Int,
    uuid: String)
    extends ProjectionContext {

  def withObserver(observer: HandlerObserver[Envelope]): ProjectionContextImpl[Offset, Envelope] =
    copy(observer = observer)

  def withExternalContext(externalContext: AnyRef): ProjectionContextImpl[Offset, Envelope] =
    copy(externalContext = externalContext)

  /**
   * scala3 makes `copy` private
   */
  def withGroupSize(groupSize: Int): ProjectionContextImpl[Offset, Envelope] = copy(groupSize = groupSize)

}

/**
 * INTERNAL API
 */
@InternalApi private[projection] object ProjectionContextImpl {
  def apply[Offset, Envelope](
      offset: Offset,
      envelope: Envelope,
      uuid: String): ProjectionContextImpl[Offset, Envelope] =
    new ProjectionContextImpl(
      offset,
      envelope,
      observer = NoopHandlerObserver,
      externalContext = null,
      groupSize = 1,
      uuid = uuid)

  def apply[Offset, Envelope](offset: Offset, envelope: Envelope): ProjectionContextImpl[Offset, Envelope] =
    apply(offset, envelope, uuid = "")
}
