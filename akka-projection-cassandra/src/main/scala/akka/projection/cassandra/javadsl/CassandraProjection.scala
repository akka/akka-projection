/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.cassandra.internal.CassandraOffsetStore
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerAdapter
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.JavaSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.AtMostOnceProjection
import akka.projection.javadsl.GroupedProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.FlowWithContext

/**
 * Factories of [[akka.projection.Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 */
object CassandraProjection {

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics. It stores the offset in Cassandra
   * after the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset some elements may be processed more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceCassandraProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]]): AtLeastOnceProjection[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new JavaSourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(() => HandlerAdapter(handler.get())),
      statusObserver = NoopStatusObserver)

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedCassandraProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in Cassandra immediately after the `handler` has processed the envelopes, but that
   * is still with at-least-once processing semantics. This means that if the projection is restarted
   * from previously stored offset the previous group of envelopes may be processed more than once.
   */
  def groupedWithin[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[java.util.List[Envelope]]]): GroupedProjection[Offset, Envelope] =
    new CassandraProjectionImpl[Offset, Envelope](
      projectionId,
      new JavaSourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy =
        AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(scala.concurrent.duration.Duration.Zero)),
      handlerStrategy = GroupedHandlerStrategy(() => new GroupedHandlerAdapter(handler.get())),
      statusObserver = NoopStatusObserver)

  /**
   * Create a [[akka.projection.Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once processing
   * semantics.
   *
   * The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried
   * in the context of the `FlowWithContext` and is stored in Cassandra when corresponding `Done` is emitted.
   * Since the offset is stored after processing the envelope it means that if the
   * projection is restarted from previously stored offset then some envelopes may be processed more than once.
   *
   * If the flow filters out envelopes the corresponding offset will not be stored, and such envelope
   * will be processed again if the projection is restarted and no later offset was stored.
   *
   * The flow should not duplicate emitted envelopes (`mapConcat`) with same offset, because then it can result in
   * that the first offset is stored and when the projection is restarted that offset is considered completed even
   * though more of the duplicated enveloped were never processed.
   *
   * The flow must not reorder elements, because the offsets may be stored in the wrong order and
   * and when the projection is restarted all envelopes up to the latest stored offset are considered
   * completed even though some of them may not have been processed. This is the reason the flow is
   * restricted to `FlowWithContext` rather than ordinary `Flow`.
   */
  def atLeastOnceFlow[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _])
      : AtLeastOnceFlowProjection[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new JavaSourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler.asScala),
      statusObserver = NoopStatusObserver)

  /**
   * Create a [[akka.projection.Projection]] with at-most-once processing semantics. It stores the offset in Cassandra
   * before the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset one envelope may not have been processed.
   */
  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Supplier[Handler[Envelope]]): AtMostOnceProjection[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new JavaSourceProviderAdapter(sourceProvider),
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtMostOnce(),
      handlerStrategy = SingleHandlerStrategy(() => HandlerAdapter(handler.get())),
      statusObserver = NoopStatusObserver)

  /**
   * For testing purposes the projection offset and management tables can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createTablesIfNotExists(system: ActorSystem[_]): CompletionStage[Done] = {
    import scala.compat.java8.FutureConverters._
    val offsetStore = new CassandraOffsetStore(system)
    offsetStore.createKeyspaceAndTable().toJava
  }

  @deprecated("Renamed to createTablesIfNotExists", "1.2.0")
  def createOffsetTableIfNotExists(system: ActorSystem[_]): CompletionStage[Done] =
    createTablesIfNotExists(system)
}
