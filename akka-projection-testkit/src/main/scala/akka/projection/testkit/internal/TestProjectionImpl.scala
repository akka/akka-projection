/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit.internal

import java.util.function.Supplier

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.SettingsImpl
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.javadsl
import akka.projection.testkit.scaladsl.TestOffsetStore
import akka.projection.testkit.scaladsl.TestProjection
import akka.stream.RestartSettings
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class TestProjectionImpl[Offset, Envelope] private[projection] (
    val projectionId: ProjectionId,
    val sourceProvider: SourceProvider[Offset, Envelope],
    val handlerStrategy: HandlerStrategy,
    val offsetStrategy: OffsetStrategy,
    val statusObserver: StatusObserver[Envelope],
    val offsetStoreFactory: () => TestOffsetStore[Offset],
    val startOffset: Option[Offset])
    extends TestProjection[Offset, Envelope]
    with akka.projection.testkit.javadsl.TestProjection[Offset, Envelope]
    with SettingsImpl[TestProjectionImpl[Offset, Envelope]] {

  // singleton state is ok because restart strategies not supported.
  // also keeps in memory offset table alive.
  private var _state: Option[TestInternalProjectionState[Offset, Envelope]] = None

  private def copy(
      projectionId: ProjectionId = projectionId,
      sourceProvider: SourceProvider[Offset, Envelope] = sourceProvider,
      handlerStrategy: HandlerStrategy = handlerStrategy,
      offsetStrategy: OffsetStrategy = offsetStrategy,
      statusObserver: StatusObserver[Envelope] = statusObserver,
      offsetStoreFactory: () => TestOffsetStore[Offset] = offsetStoreFactory,
      startOffset: Option[Offset] = startOffset): TestProjectionImpl[Offset, Envelope] =
    new TestProjectionImpl(
      projectionId,
      sourceProvider,
      handlerStrategy,
      offsetStrategy,
      statusObserver,
      offsetStoreFactory,
      startOffset)

  override def withStatusObserver(observer: StatusObserver[Envelope]): TestProjectionImpl[Offset, Envelope] =
    copy(statusObserver = observer)

  def withStartOffset(offset: Offset): TestProjectionImpl[Offset, Envelope] = copy(startOffset = Some(offset))

  def withOffsetStoreFactory(factory: () => TestOffsetStore[Offset]): TestProjectionImpl[Offset, Envelope] =
    copy(offsetStoreFactory = factory)

  override def withOffsetStoreFactory(factory: Supplier[akka.projection.testkit.javadsl.TestOffsetStore[Offset]])
      : javadsl.TestProjection[Offset, Envelope] =
    withOffsetStoreFactory(() => new TestOffsetStoreAdapter(factory.get()))

  @InternalApi
  private[projection] def withOffsetStrategy(strategy: OffsetStrategy): TestProjectionImpl[Offset, Envelope] =
    copy(offsetStrategy = strategy)

  // FIXME: Should any of the following settings be exposed by the TestProjection?
  final override def withRestartBackoffSettings(restartBackoff: RestartSettings): TestProjectionImpl[Offset, Envelope] =
    this
  override def withSaveOffset(
      afterEnvelopes: Int,
      afterDuration: FiniteDuration): TestProjectionImpl[Offset, Envelope] =
    this
  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): TestProjectionImpl[Offset, Envelope] = this

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = handlerStrategy.actorHandlerInit

  /**
   * INTERNAL API: To control the [[akka.projection.internal.InternalProjectionState]] used in the projection.
   */
  @InternalApi
  private[projection] def newState(implicit system: ActorSystem[_]): TestInternalProjectionState[Offset, Envelope] =
    new TestInternalProjectionState(
      projectionId,
      sourceProvider,
      handlerStrategy,
      offsetStrategy,
      statusObserver,
      offsetStoreFactory(),
      startOffset)

  private def state(implicit system: ActorSystem[_]): TestInternalProjectionState[Offset, Envelope] = {
    if (_state.isEmpty) _state = Some(newState)
    _state.get
  }

  override def run()(implicit system: ActorSystem[_]): RunningProjection = state.newRunningInstance()

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    state.mappedSource()
}

/**
 * INTERNAL API
 * This internal class will hold the KillSwitch that is needed
 * when building the mappedSource and when running the projection (to stop)
 */
@InternalApi
private[projection] class TestInternalProjectionState[Offset, Envelope](
    projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    handlerStrategy: HandlerStrategy,
    offsetStrategy: OffsetStrategy,
    statusObserver: StatusObserver[Envelope],
    offsetStore: TestOffsetStore[Offset],
    startOffset: Option[Offset])(implicit val system: ActorSystem[_])
    extends InternalProjectionState[Offset, Envelope](
      projectionId,
      sourceProvider,
      offsetStrategy,
      handlerStrategy,
      statusObserver,
      ProjectionSettings(system)) {

  override implicit val executionContext: ExecutionContext = system.executionContext

  startOffset.foreach(offset => offsetStore.saveOffset(projectionId, offset))

  override val logger: LoggingAdapter = Logging(system.classicSystem, this.getClass.asInstanceOf[Class[Any]])

  override def readPaused(): Future[Boolean] =
    offsetStore.readManagementState(projectionId).map(_.exists(_.paused))

  override def readOffsets(): Future[Option[Offset]] = offsetStore.readOffsets()

  override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
    offsetStore.saveOffset(projectionId, offset)

  def newRunningInstance(): RunningProjection =
    new TestRunningProjection(mappedSource(), killSwitch)

}

/**
 * INTERNAL API
 */
@InternalApi
private[projection] class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)(
    implicit val system: ActorSystem[_])
    extends RunningProjection {

  protected val futureDone: Future[Done] = run()

  protected def run(): Future[Done] = source.run()

  override def stop(): Future[Done] = {
    killSwitch.shutdown()
    futureDone
  }
}
