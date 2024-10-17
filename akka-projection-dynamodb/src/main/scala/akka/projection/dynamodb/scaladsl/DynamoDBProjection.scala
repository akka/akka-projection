/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.dynamodb.scaladsl

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.persistence.dynamodb.util.ClientProvider
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.dynamodb.DynamoDBProjectionSettings
import akka.projection.dynamodb.internal.DynamoDBProjectionImpl
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStoredByHandler
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext

@ApiMayChange
object DynamoDBProjection {

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a DynamoDB table after the `handler` has processed the envelope. This means that if the
   * projection is restarted from previously stored offset then some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first. This window
   * can be defined with [[akka.projection.scaladsl.AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForAtLeastOnce(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnce[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => DynamoDBTransactHandler[Envelope])(
      implicit
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForExactlyOnce(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[akka.projection.scaladsl.GroupedProjection.withGroup]] of the returned
   * `GroupedProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB in the same transaction as the `TransactWriteItem`s returned by the `handler`.
   */
  def exactlyOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => DynamoDBTransactHandler[Seq[Envelope]])(
      implicit
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForExactlyOnceGrouped(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes, whatever happens first. This
   * window can be defined with [[akka.projection.scaladsl.GroupedProjection.withGroup]] of the returned
   * `GroupedProjection`. The default settings for the window is defined in configuration section
   * `akka.projection.grouped`.
   *
   * The offset is stored in DynamoDB immediately after the `handler` has processed the envelopes, but that is still
   * with at-least-once processing semantics. This means that if the projection is restarted from previously stored
   * offset the previous group of envelopes may be processed more than once.
   */
  def atLeastOnceGroupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Seq[Envelope]])(implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForAtLeastOnceGrouped(sourceProvider, handler, offsetStore)(
        system.executionContext,
        system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = OffsetStoredByHandler(),
      handlerStrategy = GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[akka.stream.scaladsl.FlowWithContext]] as the envelope handler. It
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
      settings: Option[DynamoDBProjectionSettings],
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _])(
      implicit
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    val dynamodbSettings = settings.getOrElse(DynamoDBProjectionSettings(system))
    val client = ClientProvider(system).clientFor(dynamodbSettings.useClient)

    val offsetStore =
      DynamoDBProjectionImpl.createOffsetStore(
        projectionId,
        timestampOffsetBySlicesSourceProvider(sourceProvider),
        dynamodbSettings,
        client)

    val adaptedHandler =
      DynamoDBProjectionImpl.adaptedHandlerForFlow(sourceProvider, handler, offsetStore, dynamodbSettings)(system)

    new DynamoDBProjectionImpl(
      projectionId,
      dynamodbSettings,
      settingsOpt = None,
      sourceProvider,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  private def timestampOffsetBySlicesSourceProvider(
      sourceProvider: SourceProvider[_, _]): Option[BySlicesSourceProvider] = {
    sourceProvider match {
      case provider: BySlicesSourceProvider => Some(provider)
      case _                                => None // source provider is not using slices
    }
  }

}
