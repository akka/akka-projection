/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import java.util.Optional
import java.util.function.Supplier

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.compat.java8.OptionConverters._

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.InternalApi
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.SourceProviderAdapter
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source

// FIXME: this should be refactored as part of #198
@ApiMayChange
object TestProjection {
  def apply[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Envelope],
      startOffset: Option[Offset]): Projection[Envelope] =
    new TestProjection(projectionId, sourceProvider, SingleHandlerStrategy(handler), startOffset)

  /**
   * Java API
   */
  def create[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: akka.projection.javadsl.SourceProvider[Offset, Envelope],
      handler: Supplier[akka.projection.javadsl.Handler[Envelope]],
      startOffset: Optional[Offset]): Projection[Envelope] =
    new TestProjection(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      SingleHandlerStrategy(() => HandlerAdapter(handler.get())),
      startOffset.asScala)
}

@ApiMayChange
class TestProjection[Offset, Envelope] private (
    val projectionId: ProjectionId,
    sourceProvider: SourceProvider[Offset, Envelope],
    handlerStrategy: HandlerStrategy,
    startOffset: Option[Offset])
    extends Projection[Envelope]
    with SettingsImpl[TestProjection[Offset, Envelope]] {

  private var state: Option[TestInternalProjectionState] = None

  def offsetStore: TestInMemoryOffsetStore[Offset] =
    state
      .map(_.offsetStore)
      .getOrElse(throw new IllegalStateException(
        "The OffsetStore is not available because the InternalProjectionState has not been initialized (the projection has not been run)"))

  override val statusObserver: StatusObserver[Envelope] = NoopStatusObserver

  override def withStatusObserver(observer: StatusObserver[Envelope]): TestProjection[Offset, Envelope] =
    this // no need for StatusObserver in tests

  final override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): TestProjection[Offset, Envelope] = this

  override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection[Offset, Envelope] =
    this

  override def withGroup(
      groupAfterEnvelopes: Int,
      groupAfterDuration: FiniteDuration): TestProjection[Offset, Envelope] = this

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

  private def newOrExistingState(implicit system: ActorSystem[_]): TestInternalProjectionState = {
    if (state.isEmpty)
      state = Some(new TestInternalProjectionState(projectionId, sourceProvider, handlerStrategy, startOffset))
    state.get
  }

  override def run()(implicit system: ActorSystem[_]): RunningProjection =
    newOrExistingState.newRunningInstance()

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def mappedSource()(implicit system: ActorSystem[_]): Source[Done, Future[Done]] =
    newOrExistingState.mappedSource()

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  @InternalApi
  private class TestInternalProjectionState(
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerStrategy: HandlerStrategy,
      startOffset: Option[Offset])(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        // Disable batching so that `ProjectionTestKit.runWithTestSink` emits 1 `Done` per envelope.
        // FIXME: Is there any reason to let the user choose the `OffsetStrategy` in tests?
        AtLeastOnce(afterEnvelopes = Some(1)),
        handlerStrategy,
        NoopStatusObserver, // FIXME: Always disable metrics during tests?
        ProjectionSettings(system)) {

    override implicit val executionContext: ExecutionContext = system.executionContext

    val offsetStore: TestInMemoryOffsetStore[Offset] = {
      val store = new TestInMemoryOffsetStore[Offset]()
      startOffset.foreach(offset => store.saveOffset(projectionId, offset))
      store
    }

    override def logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)

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
  private class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit val system: ActorSystem[_])
      extends RunningProjection {

    private val futureDone = source.run()

    override def stop(): Future[Done] = {
      killSwitch.shutdown()
      futureDone
    }
  }
}

// FIXME: this should be replaced as part of #198
@ApiMayChange
object TestSourceProvider {
  def apply[Offset, Envelope](
      sourceEvents: List[Envelope],
      extractOffset: Envelope => Offset,
      extractCreationTime: Envelope => Long = (_: Envelope) => 0L): SourceProvider[Offset, Envelope] = {
    new TestSourceProvider[Offset, Envelope](sourceEvents, extractOffset, extractCreationTime)
  }
}

@ApiMayChange
class TestSourceProvider[Offset, Envelope] private[projection] (
    sourceEvents: List[Envelope],
    _extractOffset: Envelope => Offset,
    _extractCreationTime: Envelope => Long)
    extends SourceProvider[Offset, Envelope] {
  override def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] =
    Future.successful {
      Source(sourceEvents).concat(Source.maybe)
    }

  override def extractOffset(envelope: Envelope): Offset = _extractOffset(envelope)

  override def extractCreationTime(envelope: Envelope): Long = _extractCreationTime(envelope)
}

@ApiMayChange
class TestInMemoryOffsetStore[Offset](implicit val system: ActorSystem[_]) {
  private implicit val executionContext: ExecutionContext = system.executionContext

  private var savedOffsets = List[(ProjectionId, Offset)]()

  def readOffsets(): Future[Option[Offset]] = Future(savedOffsets.headOption.map { case (_, offset) => offset })

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }
}
