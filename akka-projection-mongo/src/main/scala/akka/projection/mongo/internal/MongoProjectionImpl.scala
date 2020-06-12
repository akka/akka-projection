/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo.internal

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
import akka.projection.mongo.TransactionCtx
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.AtLeastOnceProjection
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.GroupedProjection
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.mongodb.scala.MongoClient

@InternalApi
private[projection] class MongoProjectionImpl[Offset, Envelope](
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    db: MongoClient,
    settingsOpt: Option[ProjectionSettings],
    restartBackoffOpt: Option[RestartBackoffSettings],
    val offsetStrategy: OffsetStrategy,
    handlerStrategy: HandlerStrategy[Envelope],
    override val statusObserver: StatusObserver[Envelope],
    offsetStore: MongoOffsetStore)
    extends ExactlyOnceProjection[Offset, Envelope]
    with GroupedProjection[Offset, Envelope]
    with AtLeastOnceProjection[Offset, Envelope]
    with AtLeastOnceFlowProjection[Offset, Envelope]
    with SettingsImpl[MongoProjectionImpl[Offset, Envelope]]
    with InternalProjection {

  private def copy(
      settingsOpt: Option[ProjectionSettings] = this.settingsOpt,
      restartBackoffOpt: Option[RestartBackoffSettings] = this.restartBackoffOpt,
      offsetStrategy: OffsetStrategy = this.offsetStrategy,
      handlerStrategy: HandlerStrategy[Envelope] = this.handlerStrategy,
      statusObserver: StatusObserver[Envelope] = this.statusObserver): MongoProjectionImpl[Offset, Envelope] =
    new MongoProjectionImpl(
      projectionId,
      sourceProvider,
      db,
      settingsOpt,
      restartBackoffOpt,
      offsetStrategy,
      handlerStrategy,
      statusObserver,
      offsetStore)

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

  override def withSettings(settings: ProjectionSettings): MongoProjectionImpl[Offset, Envelope] =
    copy(settingsOpt = Option(settings))

  override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): MongoProjectionImpl[Offset, Envelope] =
    copy(restartBackoffOpt = Some(restartBackoff))

  /**
   * Settings for AtLeastOnceSlickProjection
   */
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): MongoProjectionImpl[Offset, Envelope] = {

    // safe cast: withSaveOffset is only available to AtLeastOnceProjection
    val atLeastOnce = offsetStrategy.asInstanceOf[AtLeastOnce]

    copy(offsetStrategy =
      atLeastOnce.copy(afterEnvelopes = Some(afterEnvelopes), orAfterDuration = Some(afterDuration)))
  }

  /**
   * Settings for GroupedSlickProjection
   */
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): MongoProjectionImpl[Offset, Envelope] = {

    // safe cast: withGroup is only available to GroupedProjections
    val groupedHandler = handlerStrategy.asInstanceOf[GroupedHandlerStrategy[Envelope]]

    copy(handlerStrategy =
      groupedHandler.copy(afterEnvelopes = Some(groupAfterEnvelopes), orAfterDuration = Some(groupAfterDuration)))
  }

  /**
   * Settings for AtLeastOnceSlickProjection and ExactlyOnceSlickProjection
   */
  override def withRecoveryStrategy(
      recoveryStrategy: HandlerRecoveryStrategy): MongoProjectionImpl[Offset, Envelope] = {
    val newStrategy =
      offsetStrategy match {
        case s: ExactlyOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
        case s: AtLeastOnce => s.copy(recoveryStrategy = Some(recoveryStrategy))
        //NOTE: AtMostOnce has its own withRecoveryStrategy variant
        // this method is not available for AtMostOnceProjection
        case s: AtMostOnce => s
      }
    copy(offsetStrategy = newStrategy)
  }

  override def withStatusObserver(observer: StatusObserver[Envelope]): MongoProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  /**
   * INTERNAL API
   * Return a RunningProjection
   */
  @InternalApi
  override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection = {
    new MongoInternalProjectionState(settingsOrDefaults).newRunningInstance()
  }

  /**
   * INTERNAL API
   *
   * This method returns the projection Source mapped with user 'handler' function, but before any sink attached.
   * This is mainly intended to be used by the TestKit allowing it to attach a TestSink to it.
   */
  override private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, _] =
    new MongoInternalProjectionState(settingsOrDefaults).mappedSource()

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  private class MongoInternalProjectionState(settings: ProjectionSettings)(implicit val system: ActorSystem[_])
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
      TransactionCtx.withSession(db.startSession())(offsetStore.saveOffset(projectionId, offset)).map(_ => Done)

    private[projection] def newRunningInstance(): RunningProjection =
      new MongoRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), this)

  }

  private class MongoRunningProjection(source: Source[Done, _], projectionState: MongoInternalProjectionState)(
      implicit system: ActorSystem[_])
      extends RunningProjection
      with ProjectionOffsetManagement[Offset] {

    private implicit val executionContext: ExecutionContext = system.executionContext

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
          val dbio = offsetStore.saveOffset(projectionId, o)
          TransactionCtx.withSession(db.startSession())(dbio).map(_ => Done)
        case None =>
          val dbio = offsetStore.clearOffset(projectionId)
          TransactionCtx.withSession(db.startSession())(dbio).map(_ => Done)
      }
    }
  }

}
