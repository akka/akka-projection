/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.testkit.javadsl

import java.util.function.Supplier

import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.StatusObserver
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.HandlerAdapter
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.JavaToScalaSourceProviderAdapter
import akka.projection.testkit.internal.TestInMemoryOffsetStoreImpl
import akka.projection.testkit.internal.TestProjectionImpl

object TestProjection {

  /**
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
   * @param projectionId   - a Projection ID
   * @param sourceProvider - a [[akka.projection.javadsl.SourceProvider]] to supply envelopes to the Projection
   * @param handler        - a user-defined [[akka.projection.javadsl.Handler]] to run within the Projection
   */
  def create[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: akka.projection.javadsl.SourceProvider[Offset, Envelope],
      handler: Supplier[akka.projection.javadsl.Handler[Envelope]]): TestProjection[Offset, Envelope] =
    new TestProjectionImpl(
      projectionId = projectionId,
      sourceProvider = new JavaToScalaSourceProviderAdapter(sourceProvider),
      handlerStrategy = SingleHandlerStrategy(() => new HandlerAdapter[Envelope](handler.get())),
      // Disable batching so that `ProjectionTestKit.runWithTestSink` emits 1 `Done` per envelope.
      offsetStrategy = AtLeastOnce(afterEnvelopes = Some(1)),
      statusObserver = NoopStatusObserver,
      offsetStoreFactory = () => new TestInMemoryOffsetStoreImpl[Offset](),
      startOffset = None)
}

@DoNotInherit
trait TestProjection[Offset, Envelope] extends Projection[Envelope] {
  def withStatusObserver(observer: StatusObserver[Envelope]): TestProjection[Offset, Envelope]

  /**
   * The initial offset of the offset store.
   */
  def withStartOffset(offset: Offset): TestProjection[Offset, Envelope]

  /**
   * The offset store factory. The offset store has the same lifetime as the Projection. It is instantiated when the
   * projection is first run and is created with [[newState]].
   */
  def withOffsetStoreFactory(factory: Supplier[TestOffsetStore[Offset]]): TestProjection[Offset, Envelope]

  /**
   * INTERNAL API: Choose a different [[OffsetStrategy]] for saving offsets. This is intended for Projection development only.
   */
  @InternalApi
  private[projection] def withOffsetStrategy(strategy: OffsetStrategy): TestProjection[Offset, Envelope]
}
