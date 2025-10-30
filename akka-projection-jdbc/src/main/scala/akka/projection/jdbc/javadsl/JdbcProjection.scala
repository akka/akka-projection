/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.jdbc.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.jdk.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerAdapter
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.JavaToScalaSourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.AtLeastOnceProjection
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.GroupedProjection
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.GroupedJdbcHandlerAdapter
import akka.projection.jdbc.internal.JdbcHandlerAdapter
import akka.projection.jdbc.internal.JdbcProjectionImpl
import akka.stream.javadsl.FlowWithContext

object JdbcProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using JDBC in the same transaction
   * as the user defined `handler`.
   *
   */
  def exactlyOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: Supplier[JdbcHandler[Envelope, S]],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val javaSourceProvider = new JavaToScalaSourceProviderAdapter(sourceProvider)
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForExactlyOnce(
        projectionId,
        javaSourceProvider,
        sessionFactory,
        () => new JdbcHandlerAdapter(handler.get()),
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      javaSourceProvider,
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a relational database table using JDBC after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: Supplier[JdbcHandler[Envelope, S]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForAtLeastOnce(
        sessionFactory,
        () => new JdbcHandlerAdapter(handler.get()),
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      new JavaToScalaSourceProviderAdapter(sourceProvider),
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with at-least-once processing semantics.
   *
   * Compared to [[JdbcProjection.atLeastOnce]] the [[Handler]] is not storing the projected result in the
   * database, but is integrating with something else.
   *
   * It stores the offset in a relational database table using JDBC after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset then some elements may be processed
   * more than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnceAsync[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: Supplier[Handler[Envelope]],
      system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    new JdbcProjectionImpl(
      projectionId,
      new JavaToScalaSourceProviderAdapter(sourceProvider),
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(),
      SingleHandlerStrategy(() => HandlerAdapter(handler.get())),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in a relational database table using JDBC in the same transaction
   * as the user defined `handler`.
   */
  def groupedWithin[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: Supplier[JdbcHandler[java.util.List[Envelope], S]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val javaSourceProvider = new JavaToScalaSourceProviderAdapter(sourceProvider)
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForGrouped(
        projectionId,
        javaSourceProvider,
        sessionFactory,
        () => new GroupedJdbcHandlerAdapter(handler.get()),
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      javaSourceProvider,
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      GroupedHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedProjection.withGroup]] of
   * the returned `GroupedProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * Compared to [[JdbcProjection.groupedWithin]] the [[Handler]] is not storing the projected result in the
   * database, but is integrating with something else.
   *
   * It stores the offset in  a relational database table using JDBC immediately after the `handler` has
   * processed the envelopes, but that is still with at-least-once processing semantics. This means that
   * if the projection is restarted from previously stored offset the previous group of envelopes may be
   * processed more than once.
   */
  def groupedWithinAsync[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: Supplier[Handler[java.util.List[Envelope]]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val javaSourceProvider = new JavaToScalaSourceProviderAdapter(sourceProvider)
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    new JdbcProjectionImpl(
      projectionId,
      javaSourceProvider,
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(scala.concurrent.duration.Duration.Zero)),
      GroupedHandlerStrategy(() => new GroupedHandlerAdapter(handler.get())),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * Create a [[akka.projection.Projection]] with a [[FlowWithContext]] as the envelope handler. It has at-least-once processing
   * semantics.
   *
   * The flow should emit a `Done` element for each completed envelope. The offset of the envelope is carried
   * in the context of the `FlowWithContext` and is stored in the database when corresponding `Done` is emitted.
   * Since the offset is stored after processing the envelope it means that if the
   * projection is restarted from previously stored offset then some envelopes may be processed more than once.
   *
   * If the flow filters out envelopes the corresponding offset will not be stored, and such envelope
   * will be processed again if the projection is restarted and no later offset was stored.
   *
   * The flow should not duplicate emitted envelopes (`mapConcat`) with same offset, because then it can result in
   * that the first offset is stored and when the projection is restarted that offset is considered completed even
   * though more of the duplicated envelopes were never processed.
   *
   * The flow must not reorder elements, because the offsets may be stored in the wrong order and
   * and when the projection is restarted all envelopes up to the latest stored offset are considered
   * completed even though some of them may not have been processed. This is the reason the flow is
   * restricted to `FlowWithContext` rather than ordinary `Flow`.
   */
  def atLeastOnceFlow[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Supplier[S],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.get()
    val javaSourceProvider = new JavaToScalaSourceProviderAdapter(sourceProvider)
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    new JdbcProjectionImpl(
      projectionId,
      javaSourceProvider,
      sessionFactory = sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler.asScala),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * For testing purposes the projection offset and management tables can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createTablesIfNotExists[S <: JdbcSession](
      sessionFactory: Supplier[S],
      system: ActorSystem[_]): CompletionStage[Done] =
    JdbcProjectionImpl.createOffsetStore(() => sessionFactory.get())(system).createIfNotExists().asJava

  @deprecated("Renamed to createTablesIfNotExists", "1.2.0")
  def createOffsetTableIfNotExists[S <: JdbcSession](
      sessionFactory: Supplier[S],
      system: ActorSystem[_]): CompletionStage[Done] =
    createTablesIfNotExists(sessionFactory, system)

  /**
   * For testing purposes the projection offset and management tables can be dropped programmatically.
   */
  def dropTablesIfExists[S <: JdbcSession](sessionFactory: Supplier[S], system: ActorSystem[_]): CompletionStage[Done] =
    JdbcProjectionImpl.createOffsetStore(() => sessionFactory.get())(system).dropIfExists().asJava

  @deprecated("Renamed to dropTablesIfExists", "1.2.0")
  def dropOffsetTableIfExists[S <: JdbcSession](
      sessionFactory: Supplier[S],
      system: ActorSystem[_]): CompletionStage[Done] =
    dropTablesIfExists(sessionFactory, system)
}
