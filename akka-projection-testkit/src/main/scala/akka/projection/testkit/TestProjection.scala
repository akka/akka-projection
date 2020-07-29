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

@ApiMayChange
object TestProjection {

  /**
   * Create a [[TestProjection]] that can be used to assert a [[akka.projection.scaladsl.Handler]] implementation.
   *
   * The [[TestProjection]] allows the user to test their [[akka.projection.scaladsl.Handler]] implementation in
   * isolation, without requiring the Projection implementation (i.e. a database) to exist at test runtime.
   *
   * The [[akka.projection.scaladsl.SourceProvider]] can be a concrete implementation, or a [[TestSourceProvider]] to
   * provide further test isolation.
   *
   * The [[TestProjection]] uses an at-least-once offset saving strategy where an offset is saved for each element.
   *
   * The [[TestProjection]] does not support grouping, at least once offset batching, or restart backoff strategies.
   *
   * @param projectionId - a Projection ID
   * @param sourceProvider - a [[akka.projection.scaladsl.SourceProvider]] to supply envelopes to the Projection
   * @param handler - a user-defined [[akka.projection.scaladsl.Handler]] to run within the Projection
   */
  def apply[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: () => Handler[Envelope]): Projection[Envelope] =
    new TestProjection(
      projectionId = projectionId,
      sourceProvider = sourceProvider,
      handlerStrategy = SingleHandlerStrategy(handler),
      // Disable batching so that `ProjectionTestKit.runWithTestSink` emits 1 `Done` per envelope.
      offsetStrategy = AtLeastOnce(afterEnvelopes = Some(1)),
      statusObserver = NoopStatusObserver,
      offsetStoreFactory = system => TestInMemoryOffsetStore[Offset](system),
      startOffset = None)

  /**
   * Java API
   *
   * Create a [[TestProjection]] that can be used to assert a [[akka.projection.javadsl.Handler]] implementation.
   *
   * The [[TestProjection]] allows the user to test their [[akka.projection.javadsl.Handler]] implementation in
   * isolation, without requiring the Projection implementation (i.e. a database) to exist at test runtime.
   *
   * The [[akka.projection.javadsl.SourceProvider]] can be a concrete implementation, or a [[TestSourceProvider]] to
   * provide further test isolation.
   *
   * The [[TestProjection]] uses an at-least-once offset saving strategy where an offset is saved for each element.
   *
   * The [[TestProjection]] does not support grouping, at least once offset batching, or restart backoff strategies.
   *
   * @param projectionId - a Projection ID
   * @param sourceProvider - a [[akka.projection.javadsl.SourceProvider]] to supply envelopes to the Projection
   * @param handler - a user-defined [[akka.projection.javadsl.Handler]] to run within the Projection
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
    val offsetStrategy: OffsetStrategy,
    val statusObserver: StatusObserver[Envelope],
    val offsetStoreFactory: ActorSystem[_] => TestInMemoryOffsetStore[Offset],
    val startOffset: Option[Offset])
    extends Projection[Envelope]
    with SettingsImpl[TestProjection[Offset, Envelope]] {

  // singleton state is ok because restart strategies not supported.
  // also keeps in memory offset table alive.
  private var _state: Option[TestInternalProjectionState] = None

  private def copy(
      projectionId: ProjectionId = projectionId,
      sourceProvider: SourceProvider[Offset, Envelope] = sourceProvider,
      handlerStrategy: HandlerStrategy = handlerStrategy,
      offsetStrategy: OffsetStrategy = offsetStrategy,
      statusObserver: StatusObserver[Envelope] = statusObserver,
      offsetStoreFactory: ActorSystem[_] => TestInMemoryOffsetStore[Offset] = offsetStoreFactory,
      startOffset: Option[Offset] = startOffset): TestProjection[Offset, Envelope] =
    new TestProjection(
      projectionId,
      sourceProvider,
      handlerStrategy,
      offsetStrategy,
      statusObserver,
      offsetStoreFactory,
      startOffset)

  override def withStatusObserver(observer: StatusObserver[Envelope]): TestProjection[Offset, Envelope] =
    copy(statusObserver = observer)

  /**
   * The initial offset of the offset store.
   */
  def withStartOffset(offset: Offset): TestProjection[Offset, Envelope] = copy(startOffset = Some(offset))

  /**
   * The offset store factory. The offset store has the same lifetime as the Projection. It is instantiated when a
   * new [[InternalProjectionState]] is created with [[newState]].
   */
  def withOffsetStoreFactory(
      factory: ActorSystem[_] => TestInMemoryOffsetStore[Offset]): TestProjection[Offset, Envelope] =
    copy(offsetStoreFactory = factory)

  /**
   * INTERNAL API: Choose a different [[OffsetStrategy]] for saving offsets. This is intended for Projection development only.
   */
  @InternalApi
  private[projection] def withOffsetStrategy(strategy: OffsetStrategy): TestProjection[Offset, Envelope] =
    copy(offsetStrategy = strategy)

  // FIXME: Should any of the following settings be exposed by the TestProjection?
  final override def withRestartBackoffSettings(
      restartBackoff: RestartBackoffSettings): TestProjection[Offset, Envelope] =
    this
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

  /**
   * INTERNAL API: To control the [[InternalProjectionState]] used in the projection.
   */
  @InternalApi
  private[projection] def newState(implicit system: ActorSystem[_]): TestInternalProjectionState =
    new TestInternalProjectionState(
      projectionId,
      sourceProvider,
      handlerStrategy,
      offsetStrategy,
      offsetStoreFactory(system),
      startOffset)

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

  /**
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
      offsetStore: TestInMemoryOffsetStore[Offset],
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

    protected val futureDone: Future[Done] = source.run()

    override def stop(): Future[Done] = {
      killSwitch.shutdown()
      futureDone
    }
  }
}

@ApiMayChange
object TestSourceProvider {

  /**
   * A [[TestSourceProvider]] is used to supply an arbitrary stream of envelopes to a [[TestProjection]]
   *
   * @param sourceEvents - a [[akka.stream.scaladsl.Source]] of envelopes
   * @param extractOffset - a user-defined function to extract the offset from an envelope.
   */
  def apply[Offset, Envelope](
      sourceEvents: Source[Envelope, NotUsed],
      extractOffset: Envelope => Offset): TestSourceProvider[Offset, Envelope] = {
    new TestSourceProvider[Offset, Envelope](
      sourceEvents = sourceEvents,
      extractOffsetFn = extractOffset,
      extractCreationTimeFn = (_: Envelope) => 0L,
      allowCompletion = false)
  }

  /**
   * A [[TestSourceProvider]] is used to supply an arbitrary stream of envelopes to a [[TestProjection]]
   *
   * @param sourceEvents - a [[akka.stream.javadsl.Source]] of envelopes
   * @param extractOffset - a user-defined function to extract the offset from an envelope
   */
  def create[Offset, Envelope](
      sourceEvents: akka.stream.javadsl.Source[Envelope, NotUsed],
      extractOffset: java.util.function.Function[Envelope, Offset]): TestSourceProvider[Offset, Envelope] =
    apply(sourceEvents.asScala, extractOffset.asScala)
}

@ApiMayChange
class TestSourceProvider[Offset, Envelope] private (
    sourceEvents: Source[Envelope, NotUsed],
    extractOffsetFn: Envelope => Offset,
    extractCreationTimeFn: Envelope => Long,
    allowCompletion: Boolean)
    extends SourceProvider[Offset, Envelope] {

  /**
   * A user-defined function to extract the event creation time from an envelope.
   */
  def withExtractCreationTimeFunction(extractCreationTimeFn: Envelope => Long): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(sourceEvents, extractOffsetFn, extractCreationTimeFn, allowCompletion)

  /**
   * Java API
   *
   * A user-defined function to extract the event creation time from an envelope.
   */
  def withExtractCreationTimeFunction(
      extractCreationTime: java.util.function.Function[Envelope, Long]): TestSourceProvider[Offset, Envelope] =
    new TestSourceProvider(sourceEvents, extractOffsetFn, extractCreationTime.asScala, allowCompletion)

  /**
   * Allow the [[sourceEvents]] Source to complete or stay open indefinitely.
   */
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
object TestInMemoryOffsetStore {

  /**
   * An in-memory offset store that may be used with a [[TestProjection]].
   *
   * @param system - an [[akka.actor.typed.ActorSystem]] to provide an execution context to asynchronous operations.
   */
  def apply[Offset](system: ActorSystem[_]): TestInMemoryOffsetStore[Offset] =
    new TestInMemoryOffsetStore[Offset](system)

  /**
   * An in-memory offset store that may be used with a [[TestProjection]].
   *
   * @param system - an [[akka.actor.typed.ActorSystem]] to provide an execution context to asynchronous operations.
   */
  def create[Offset](system: ActorSystem[_]): TestInMemoryOffsetStore[Offset] = apply(system)
}

@ApiMayChange
class TestInMemoryOffsetStore[Offset] private (system: ActorSystem[_]) {
  private implicit val executionContext: ExecutionContext = system.executionContext

  private var savedOffsets = List[(ProjectionId, Offset)]()

  /**
   * The last saved offset to the offset store.
   */
  def lastOffset(): Option[Offset] = savedOffsets.headOption.map { case (_, offset) => offset }

  /**
   * All offsets saved to the offset store.
   */
  def allOffsets(): List[(ProjectionId, Offset)] = savedOffsets

  def readOffsets(): Future[Option[Offset]] = Future(lastOffset())

  def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = {
    savedOffsets = (projectionId -> offset) +: savedOffsets
    Future.successful(Done)
  }
}
