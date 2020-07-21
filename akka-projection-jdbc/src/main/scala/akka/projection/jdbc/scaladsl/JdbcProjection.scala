/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.scaladsl

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.JdbcProjectionImpl
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext

@ApiMayChange
object JdbcProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using JDBC in the same transaction
   * as the user defined `handler`.
   */
  def exactlyOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handler: () => JdbcHandler[Envelope, S])(
      implicit system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForExactlyOnce(
        projectionId,
        sourceProvider,
        sessionFactory,
        handler,
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
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
   * The [[JdbcHandler.process()]] in [[handler]] will be wrapped in a transaction. It is highly recommended to use
   * a [[sessionFactory]] that provides [[java.sql.Connection]]'s with [[setAutoCommit(false)]]. The transaction
   * will be committed after invoking [[JdbcHandler.process()]].
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceProjection.withSaveOffset]] of the returned
   * `AtLeastOnceProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
   */
  def atLeastOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handler: () => JdbcHandler[Envelope, S])(
      implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForAtLeastOnce(sessionFactory, handler, offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(adaptedHandler),
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
      sessionFactory: () => S,
      handler: () => Handler[Envelope])(implicit system: ActorSystem[_]): AtLeastOnceProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = SingleHandlerStrategy(handler),
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
      sessionFactory: () => S,
      handler: () => JdbcHandler[immutable.Seq[Envelope], S])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForGrouped(projectionId, sourceProvider, sessionFactory, handler, offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = ExactlyOnce(),
      handlerStrategy = GroupedHandlerStrategy(adaptedHandler),
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
      sessionFactory: () => S,
      handler: () => Handler[immutable.Seq[Envelope]])(
      implicit system: ActorSystem[_]): GroupedProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(Duration.Zero)),
      handlerStrategy = GroupedHandlerStrategy(handler),
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
      sessionFactory: () => S,
      handler: FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _])(
      implicit system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)
    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      offsetStrategy = AtLeastOnce(),
      handlerStrategy = FlowHandlerStrategy(handler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists[S <: JdbcSession](sessionFactory: () => S)(
      implicit system: ActorSystem[_]): Future[Done] =
    JdbcProjectionImpl.createOffsetStore(sessionFactory).createIfNotExists()

}
