/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.japi.function.Creator
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.GroupedProjection
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
      sessionCreator: Creator[S],
      handler: Supplier[JdbcHandler[Envelope, S]],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.create()
    val javaSourceProvider = new SourceProviderAdapter(sourceProvider)
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
      sessionCreator: Creator[S],
      handler: Supplier[JdbcHandler[java.util.List[Envelope], S]],
      system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.create()
    val javaSourceProvider = new SourceProviderAdapter(sourceProvider)
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
  def atLeastOnceFlow[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionCreator: Creator[S],
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.create()
    val javaSourceProvider = new SourceProviderAdapter(sourceProvider)
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
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists[S <: JdbcSession](
      sessionFactory: () => S,
      system: ActorSystem[_]): CompletionStage[Done] =
    JdbcProjectionImpl.createOffsetStore(sessionFactory)(system).createIfNotExists().toJava

}
