/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.ProjectionOffsetManagement
import akka.projection.RunningProjection
import akka.projection.RunningProjection.AbortProjectionException
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjection
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.projection.javadsl
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.scaladsl.JdbcHandler
import akka.projection.scaladsl
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object JdbcProjectionImpl {

  private[projection] def createOffsetStore[S <: JdbcSession](sessionFactory: () => S)(
      implicit system: ActorSystem[_]) =
    new JdbcOffsetStore[S](JdbcSettings(system), sessionFactory)

  private[projection] def adaptedHandlerForExactlyOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handlerFactory: () => JdbcHandler[Envelope, S],
      offsetStore: JdbcOffsetStore[S]): () => Handler[Envelope] = { () =>
    new Handler[Envelope] {
      private val delegate = handlerFactory()

      override def process(envelope: Envelope): Future[Done] = {
        val offset = sourceProvider.extractOffset(envelope)
        // this scope ensures that the blocking DB dispatcher is used solely for DB operations
        implicit val executionContext: ExecutionContext = offsetStore.executionContext
        JdbcSessionUtil
          .withSession(sessionFactory) { sess =>
            sess.withConnection[Unit] { conn =>
              offsetStore.saveOffsetBlocking(conn, projectionId, offset)
            }
            // run users handler
            delegate.process(sess, envelope)
          }
          .map(_ => Done)

      }

      override def start(): Future[Done] = delegate.start()
      override def stop(): Future[Done] = delegate.stop()
    }
  }

  private[projection] def adaptedHandlerForGrouped[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handlerFactory: () => JdbcHandler[immutable.Seq[Envelope], S],
      offsetStore: JdbcOffsetStore[S]): () => Handler[immutable.Seq[Envelope]] = { () =>

    new Handler[immutable.Seq[Envelope]] {

      private val delegate = handlerFactory()

      override def process(envelopes: immutable.Seq[Envelope]): Future[Done] = {
        val offset = sourceProvider.extractOffset(envelopes.last)
        // this scope ensures that the blocking DB dispatcher is used solely for DB operations
        implicit val executionContext: ExecutionContext = offsetStore.executionContext
        JdbcSessionUtil
          .withSession(sessionFactory) { sess =>
            sess.withConnection[Unit] { conn =>
              offsetStore.saveOffsetBlocking(conn, projectionId, offset)
            }
            // run users handler
            delegate.process(sess, envelopes)
          }
          .map(_ => Done)

      }

      override def start(): Future[Done] = delegate.start()
      override def stop(): Future[Done] = delegate.stop()
    }
  }
}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class JdbcProjectionImpl[Offset, Envelope, S <: JdbcSession](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    sessionFactory: () => S,
    settingsOpt: Option[ProjectionSettings],
    restartBackoffOpt: Option[RestartBackoffSettings],
    val offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy,
    override val statusObserver: StatusObserver[Envelope],
    offsetStore: JdbcOffsetStore[S])
    extends scaladsl.ExactlyOnceProjection[Offset, Envelope]
    with javadsl.ExactlyOnceProjection[Offset, Envelope]
    with scaladsl.GroupedProjection[Offset, Envelope]
    with javadsl.GroupedProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceProjection[Offset, Envelope]
    with javadsl.AtLeastOnceProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with javadsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with SettingsImpl[JdbcProjectionImpl[Offset, Envelope, S]]
    with InternalProjection {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartBackoffSettings] = this.restartBackoffOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: HandlerStrategy = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): JdbcProjectionImpl[Offset, Envelope, S] =
    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt,
      restartBackoffOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver,
      offsetStore)

  type ReadOffset = () => Future[Option[Offset]]

  /*
   * Build the final ProjectionSettings to use, if currently set to None fallback to values in config file
   */
  private def settingsOrDefaults(implicit system: ActorSystem[_]): ProjectionSettings = {
    val settings = settingsOpt.getOrElse(ProjectionSettings(system))
    restartBackoffOpt match {
      case None    => settings
      case Some(r) => settings.copy(restartBackoff = r)
    }
  }

  override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(restartBackoffOpt = Some(restartBackoff))

  /**
   * Settings for AtLeastOnceSlickProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(offsetStrategy = offsetStrategy
      .asInstanceOf[AtLeastOnce]
      .copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))

  /**
   * Settings for GroupedSlickProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(handlerStrategy = handlerStrategy
      .asInstanceOf[GroupedHandlerStrategy[Envelope]]
      .copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): JdbcProjectionImpl[Offset, Envelope, S] = {
    val newStrategy = offsetStrategy match {
      case s: ExactlyOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      case s: AtLeastOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
      //NOTE: AtMostOnce has its own withRecoveryStrategy variant
      // this method is not available for AtMostOnceProjection
      case s: AtMostOnce => s
    }
    copy(offsetStrategy = newStrategy)
  }

  override def withStatusObserver(observer: StatusObserver[Envelope]): JdbcProjectionImpl[Offset, Envelope, S] =
    copy(statusObserver = observer)

  private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] =
    handlerStrategy.actorHandlerInit

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  override private[projection] def run()(implicit system: ActorSystem[_]) =
    new JdbcInternalProjectionState(settingsOrDefaults).newRunningInstance()

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    new JdbcInternalProjectionState(settingsOrDefaults).mappedSource()

  private class JdbcInternalProjectionState(settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {

    implicit val executionContext: ExecutionContext = system.executionContext
    override def logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)

    override def readOffsets(): Future[Option[Offset]] =
      offsetStore.readOffset(projectionId)

    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
      offsetStore.saveOffset(projectionId, offset)

    private[projection] def newRunningInstance(): RunningProjection =
      new JdbcRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)
  }

  private class JdbcRunningProjection(source: Source[Done, _], projectionState: JdbcInternalProjectionState)(
      implicit system: ActorSystem[_])
      extends RunningProjection
      with ProjectionOffsetManagement[Offset] {

    private val streamDone = source.run()

    override def stop(): Future[Done] = {
      projectionState.killSwitch.shutdown()
      // if the handler is retrying it will be aborted by this,
      // otherwise the stream would not be completed by the killSwitch until after all retries
      projectionState.abort.failure(AbortProjectionException)
      streamDone
    }

    // ProjectionOffsetManagement
    override def getOffset(): Future[Option[Offset]] = {
      offsetStore.readOffset(projectionId)
    }

    // ProjectionOffsetManagement
    override def setOffset(offset: Option[Offset]): Future[Done] = {
      offset match {
        case Some(o) => offsetStore.saveOffset(projectionId, o)
        case None    => offsetStore.clearOffset(projectionId)
      }
    }
  }

}
