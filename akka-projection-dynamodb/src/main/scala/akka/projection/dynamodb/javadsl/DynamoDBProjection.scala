/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.javadsl

import java.util.Optional
import java.util.function.Supplier

import scala.jdk.OptionConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBTransactGroupedHandlerAdapter
import akka.projection.dynamodb.internal.DynamoDBTransactHandlerAdapter
import akka.projection.dynamodb.scaladsl
import akka.projection.internal.GroupedHandlerAdapter
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.JavaToScalaBySliceSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.GroupedProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.FlowWithContext

@ApiMayChange
object DynamoDBProjection {

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a DynamoDB table after the `handler` has processed the envelope. This means that if the
   * projection is restarted from previously stored offset then some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[akka.projection.javadsl.AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    scaladsl.DynamoDBProjection
      .atLeastOnce[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => HandlerAdapter(handler.get()))(system)
      .asInstanceOf[AtLeastOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[DynamoDBTransactHandler[Envelope]],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    scaladsl.DynamoDBProjection
      .exactlyOnce[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new DynamoDBTransactHandlerAdapter(handler.get()))(system)
      .asInstanceOf[ExactlyOnceProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[akka.projection.javadsl.GroupedProjection.withGroup]] of the returned
   * `GroupedProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[DynamoDBTransactHandler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.DynamoDBProjection
      .exactlyOnceGroupedWithin[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new DynamoDBTransactGroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[akka.projection.javadsl.GroupedProjection.withGroup]] of the returned
   * `GroupedProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB immediately after the `handler` has processed the envelopes, but that is still
   * with at-least-once processing semantics. This means that if the projection is restarted from previously stored
   * offset the previous group of envelopes may be processed more than once.
   */
  def atLeastOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {
    scaladsl.DynamoDBProjection
      .atLeastOnceGroupedWithin[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter(sourceProvider),
        () => new GroupedHandlerAdapter(handler.get()))(system)
      .asInstanceOf[GroupedProjection[Offset, Envelope]]
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[akka.stream.javadsl.FlowWithContext]] as the envelope handler. It
   * has at-least-once processing semantics.
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
   * The flow must not reorder elements, because the offsets may be stored in the wrong order and when the projection is
   * restarted all envelopes up to the latest stored offset are considered completed even though some of them may not
   * have been processed. This is the reason the flow is restricted to `FlowWithContext` rather than ordinary `Flow`.
   */
  def atLeastOnceFlow[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Optional[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {
    scaladsl.DynamoDBProjection
      .atLeastOnceFlow[Offset, Envelope](
        projectionId,
        settings.toScala,
        JavaToScalaBySliceSourceProviderAdapter[Offset, Envelope](sourceProvider),
        handler.asScala)(system)
      .asInstanceOf[AtLeastOnceFlowProjection[Offset, Envelope]]
  }

}
