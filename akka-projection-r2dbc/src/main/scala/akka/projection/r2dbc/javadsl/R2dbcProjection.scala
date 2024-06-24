/*
 * Copyright (C) 2022 - 2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.javadsl

import java.util.Optional
import java.util.function.Supplier
import scala.compat.java8.OptionConverters._
import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.internal.GroupedHandlerAdapter
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.JavaToScalaBySliceSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.GroupedProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.internal.R2dbcGroupedHandlerAdapter
import akka.projection.r2dbc.internal.R2dbcHandlerAdapter
import akka.projection.r2dbc.scaladsl
import akka.stream.javadsl.FlowWithContext

@ApiMayChange
object R2dbcProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using R2DBC in the same transaction as the user defined
   * `handler`.
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[R2dbcHandler[Envelope]],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {
    scaladsl.R2dbcProjection
      .exactlyOnce[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new R2dbcHandlerAdapter(handler.get()))(system)
      .asInstanceOf[ExactlyOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a relational database table using R2DBC after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The [[R2dbcHandler.process]] in `handler` will be wrapped in a transaction. The transaction will be committed after
   * invoking [[R2dbcHandler.process]].
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned `AtLeastOnceProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[R2dbcHandler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {
    scaladsl.R2dbcProjection
      .atLeastOnce[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new R2dbcHandlerAdapter(handler.get()))(system)
      .asInstanceOf[AtLeastOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * Compared to [[R2dbcProjection.atLeastOnce]] the [[Handler]] is not storing the projected result in the database,
   * but is integrating with something else.
   *
   * It stores the offset in a relational database table using R2DBC after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned `AtLeastOnceProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.at-least-once`.
   */
  def atLeastOnceAsync[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    scaladsl.R2dbcProjection
      .atLeastOnceAsync[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => HandlerAdapter(handler.get()))(system)
      .asInstanceOf[AtLeastOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[GroupedProjection.withGroup]] of the returned `GroupedProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.grouped`.
   *
   * It stores the offset in a relational database table using R2DBC in the same transaction as the user defined
   * `handler`.
   */
  def groupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[R2dbcHandler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.R2dbcProjection
      .groupedWithin[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new R2dbcGroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[GroupedProjection.withGroup]] of the returned `GroupedProjection`. The default
   * settings for the window is defined in configuration section `akka.projection.grouped`.
   *
   * Compared to [[R2dbcProjection.groupedWithin]] the [[Handler]] is not storing the projected result in the database,
   * but is integrating with something else.
   *
   * It stores the offset in a relational database table using R2DBC immediately after the `handler` has processed the
   * envelopes, but that is still with at-least-once processing semantics. This means that if the projection is
   * restarted from previously stored offset the previous group of envelopes may be processed more than once.
   */
  def groupedWithinAsync[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.R2dbcProjection
      .groupedWithinAsync[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new GroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once
   * processing semantics.
   *
   * The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried in the
   * context of the `FlowWithContext` and is stored in the database when corresponding `Done` is emitted. Since the
   * offset is stored after processing the envelope it means that if the projection is restarted from previously stored
   * offset then some envelopes may be processed more than once.
   *
   * If the flow filters out envelopes the corresponding offset will not be stored, and such envelope will be processed
   * again if the projection is restarted and no later offset was stored.
   *
   * The flow should not duplicate emitted envelopes (`mapConcat`) with same offset, because then it can result in that
   * the first offset is stored and when the projection is restarted that offset is considered completed even though
   * more of the duplicated envelopes were never processed.
   *
   * The flow must not reorder elements, because the offsets may be stored in the wrong order and when the
   * projection is restarted all envelopes up to the latest stored offset are considered completed even though some of
   * them may not have been processed. This is the reason the flow is restricted to `FlowWithContext` rather than
   * ordinary `Flow`.
   */
  def atLeastOnceFlow[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[R2dbcProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {
    scaladsl.R2dbcProjection
      .atLeastOnceFlow[Offset, Envelope](
        projectionId,
        settings.asScala,
        JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope](sourceProvider),
        handler.asScala)(system)
      .asInstanceOf[AtLeastOnceFlowProjection[Offset, Envelope]]
  }

}
