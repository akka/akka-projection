/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.internal

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
import akka.projection.StrictRecoveryStrategy
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.AtMostOnce
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjection
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.projection.javadsl
import akka.projection.scaladsl
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi private[akka] class CassandraProjectionImpl[Offset, Envelope](
    override val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    settingsOpt: Option[ProjectionSettings],
    restartBackoffOpt: Option[RestartBackoffSettings],
    val offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy[Envelope],
    override val statusObserver: StatusObserver[Envelope])
    extends scaladsl.AtLeastOnceProjection[Offset, Envelope]
    with javadsl.AtLeastOnceProjection[Offset, Envelope]
    with scaladsl.GroupedProjection[Offset, Envelope]
    with javadsl.GroupedProjection[Offset, Envelope]
    with scaladsl.AtMostOnceProjection[Offset, Envelope]
    with javadsl.AtMostOnceProjection[Offset, Envelope]
    with scaladsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with javadsl.AtLeastOnceFlowProjection[Offset, Envelope]
    with SettingsImpl[CassandraProjectionImpl[Offset, Envelope]]
    with InternalProjection {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartBackoffSettings] = this.restartBackoffOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: HandlerStrategy[Envelope] = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): CassandraProjectionImpl[Offset, Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      sourceProvider,
      settingsOpt,
      restartBackoffOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver)

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

  override def withSettings(settings: ProjectionSettings): CassandraProjectionImpl[Offset, Envelope] =
    copy(settingsOpt = Option(settings))

  override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): CassandraProjectionImpl[Offset, Envelope] =
    copy(restartBackoffOpt = Some(restartBackoff))

  /**
   * Settings for AtLeastOnceCassandraProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] = {

    // safe cast: withSaveOffset is only available to AtLeastOnceProjection
    val atLeastOnce = offsetStrategy.asInstanceOf[AtLeastOnce]

    copy(offsetStrategy =
      atLeastOnce.copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))
  }

  /**
   * Settings for GroupedCassandraProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): CassandraProjectionImpl[Offset, Envelope] = {

    // safe cast: withGroup is only available to GroupedProjections
    val groupedHandler = handlerStrategy.asInstanceOf[GroupedHandlerStrategy[Envelope]]

    copy(handlerStrategy =
      groupedHandler.copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))
  }

  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] = {

    // safe cast: withRecoveryStrategy(HandlerRecoveryStrategy) is only available to AtLeastOnceProjection
    val atLeastOnce = offsetStrategy.asInstanceOf[AtLeastOnce]

    copy(offsetStrategy = atLeastOnce.copy(recoveryStrategy = Some(recoveryStrategy)))
  }

  /**
   * Settings for AtMostOnceCassandraProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: StrictRecoveryStrategy): CassandraProjectionImpl[Offset, Envelope] = {

    // safe cast: withRecoveryStrategy(StrictRecoveryStrategy) is only available to AtMostOnceProjection
    val atMostOnce = offsetStrategy.asInstanceOf[AtMostOnce]

    copy(offsetStrategy = atMostOnce.copy(Some(recoveryStrategy)))
  }

  override def withStatusObserver(observer: StatusObserver[Envelope]): CassandraProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] =
    handlerStrategy.actorHandlerInit

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection = {
    new CassandraInternalProjectionState(settingsOrDefaults).newRunningInstance()
  }

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  @InternalApi
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] = {
    new CassandraInternalProjectionState(settingsOrDefaults).mappedSource()
  }

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class CassandraInternalProjectionState(val settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {

    // FIXME maybe use the session-dispatcher config
    override implicit def executionContext: ExecutionContext = system.executionContext
    override def logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)

    private val offsetStore = new CassandraOffsetStore(system)

    override def readOffsets(): Future[Option[Offset]] =
      offsetStore.readOffset(projectionId)

    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
      offsetStore.saveOffset(projectionId, offset)

    private[projection] def newRunningInstance(): RunningProjection = {
      new CassandraRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), offsetStore, this)
    }

  }

  private class CassandraRunningProjection(
      source: Source[Done, _],
      offsetStore: CassandraOffsetStore,
      projectionState: CassandraInternalProjectionState)(implicit system: ActorSystem[_])
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
        case Some(o) =>
          offsetStore.saveOffset(projectionId, o)
        case None =>
          offsetStore.clearOffset(projectionId)
      }
    }

  }

}
