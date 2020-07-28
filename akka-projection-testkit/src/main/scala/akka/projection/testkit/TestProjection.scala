/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.testkit

import java.util.function.Supplier

import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

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
import akka.projection.internal.OffsetStrategy
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
      handler: () => Handler[Envelope]): Projection[Envelope] =
    new TestProjection(
      projectionId,
      sourceProvider,
      SingleHandlerStrategy(handler),
      AtLeastOnce(afterEnvelopes = Some(1)),
      None)

  /**
   * Java API
   */
  def create[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: akka.projection.javadsl.SourceProvider[Offset, Envelope],
      handler: Supplier[akka.projection.javadsl.Handler[Envelope]]): Projection[Envelope] =
    apply(projectionId, new SourceProviderAdapter(sourceProvider), () => HandlerAdapter(handler.get()))
}

@ApiMayChange
class TestProjection[Offset, Envelope] private[projection] (
    val projectionId: ProjectionId,
    val sourceProvider: SourceProvider[Offset, Envelope],
    val handlerStrategy: HandlerStrategy,
    // Disable batching so that `ProjectionTestKit.runWithTestSink` emits 1 `Done` per envelope.
    val offsetStrategy: OffsetStrategy,
    val startOffset: Option[Offset])
    extends Projection[Envelope]
    with SettingsImpl[TestProjection[Offset, Envelope]] {

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

  def withStartOffset(offset: Offset): TestProjection[Offset, Envelope] =
    new TestProjection(projectionId, sourceProvider, handlerStrategy, offsetStrategy, Some(offset))

  // FIXME: Consider opening up `OffsetStrategy` for testkit?
  private[projection] def withOffsetStrategy(strategy: OffsetStrategy): TestProjection[Offset, Envelope] =
    new TestProjection(projectionId, sourceProvider, handlerStrategy, strategy, startOffset)

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

  private var _state: Option[TestInternalProjectionState] = None

  /**
   * INTERNAL API
   */
  @InternalApi
  private[projection] def newState(implicit system: ActorSystem[_]): TestInternalProjectionState =
    new TestInternalProjectionState(projectionId, sourceProvider, handlerStrategy, offsetStrategy, startOffset)

  private def state(implicit system: ActorSystem[_]): TestInternalProjectionState = {
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

  /*
   * INTERNAL API
   * This internal class will hold the KillSwitch that is needed
   * when building the mappedSource and when running the projection (to stop)
   */
  @InternalApi
  private[projection] class TestInternalProjectionState(
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handlerStrategy: HandlerStrategy,
      offsetStrategy: OffsetStrategy,
      startOffset: Option[Offset])(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        // FIXME: Always disable metrics during tests?
        NoopStatusObserver,
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
  private[projection] class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)(
      implicit val system: ActorSystem[_])
      extends RunningProjection {

    protected val futureDone = source.run()

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
      sourceEvents: Source[Envelope, NotUsed],
      extractOffset: Envelope => Offset): TestSourceProvider[Offset, Envelope] = {
    new TestSourceProvider[Offset, Envelope](
      sourceEvents = sourceEvents,
      extractOffsetFn = extractOffset,
      extractCreationTimeFn = (_: Envelope) => 0L,
      allowCompletion = false)
  }

  // TODO: Java API
}

@ApiMayChange
class TestSourceProvider[Offset, Envelope] private (
    sourceEvents: Source[Envelope, NotUsed],
    extractOffsetFn: Envelope => Offset,
    extractCreationTimeFn: Envelope => Long,
    allowCompletion: Boolean)
    extends SourceProvider[Offset, Envelope] {

  def withExtractCreationTimeFunction(extractCreationTimeFn: Envelope => Long): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(sourceEvents, extractOffsetFn, extractCreationTimeFn, allowCompletion)

  /**
   * Java API
   */
  def withExtractCreationTimeFunction(
      extractCreationTime: java.util.function.Function[Envelope, Long]): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(sourceEvents, extractOffsetFn, extractCreationTime.asScala, allowCompletion)

  def withAllowCompletion(allowCompletion: Boolean): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(sourceEvents, extractOffsetFn, extractCreationTimeFn, allowCompletion)

  override def source(offset: () => Future[Option[Offset]]): Future[Source[Envelope, NotUsed]] =
    Future.successful {
      if (allowCompletion) sourceEvents
      else sourceEvents.concat(Source.maybe)
    }

  override def extractOffset(envelope: Envelope): Offset = extractOffsetFn(envelope)

  override def extractCreationTime(envelope: Envelope): Long = extractCreationTimeFn(envelope)
}

@ApiMayChange
class TestInMemoryOffsetStore[Offset](implicit val system: ActorSystem[_]) {
  private implicit val executionContext: ExecutionContext = system.executionContext

  private var savedOffsets = List[(ProjectionId, Offset)]()

  def lastOffset(): Option[Offset] = savedOffsets.headOption.map { case (_, offset) => offset }

  def readOffsets(): Future[Option[Offset]] = Future(lastOffset())

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }
}
