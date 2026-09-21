/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.projection.ProjectionId
import akka.projection.StatusObserver
import akka.projection.TestStatusObserver
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.Envelope
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec.InMemInternalProjectionState
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.internal.TestInMemoryOffsetStoreImpl
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

class InternalProjectionStateStopSpec extends ScalaTestWithActorTestKit("""
    # much longer than the expected stop time
    akka.projection.restart-backoff {
      min-backoff = 30s
      max-backoff = 30s
    }
    """) with AnyWordSpecLike with LogCapturing {

  private class TestSourceProvider(result: () => Future[Source[Envelope, NotUsed]])
      extends SourceProvider[Long, Envelope] {
    val sourceCalls = new AtomicInteger

    override def source(offset: () => Future[Option[Long]]): Future[Source[Envelope, NotUsed]] = {
      sourceCalls.incrementAndGet()
      result()
    }
    override def extractOffset(envelope: Envelope): Long = envelope.offset
    override def extractCreationTime(envelope: Envelope): Long = envelope.creationTimestamp
  }

  private def projectionState(
      sourceProvider: SourceProvider[Long, Envelope],
      offsetStore: TestInMemoryOffsetStoreImpl[Long] = new TestInMemoryOffsetStoreImpl[Long](),
      processEnvelope: Envelope => Future[Done] = _ => Future.successful(Done),
      statusObserver: StatusObserver[Envelope] = NoopStatusObserver) = {
    val handler = new Handler[Envelope] {
      override def process(envelope: Envelope): Future[Done] = processEnvelope(envelope)
    }
    new InMemInternalProjectionState[Long, Envelope](
      ProjectionId("stop-spec", sourceProvider.hashCode.toString),
      sourceProvider,
      AtLeastOnce(),
      SingleHandlerStrategy(() => handler),
      statusObserver,
      ProjectionSettings(system),
      offsetStore)
  }

  "Stopping a projection" must {

    "complete without waiting for a pending restart backoff" in {
      val sourceProvider = new TestSourceProvider(() => Future.failed(new RuntimeException("source failure")))
      val statusProbe = createTestProbe[TestStatusObserver.Status]()
      val running =
        projectionState(
          sourceProvider,
          statusObserver = new TestStatusObserver[Envelope](statusProbe.ref, lifecycle = true))
          .newRunningInstance()
      statusProbe.expectMessage(TestStatusObserver.Started)
      // the stream has failed and the restart backoff is pending
      statusProbe.expectMessage(TestStatusObserver.Failed)
      statusProbe.expectMessage(TestStatusObserver.Stopped)

      running.stop().futureValue(timeout(5.seconds)) should ===(Done)
      sourceProvider.sourceCalls.get should ===(1)
    }

    "complete while the source is still starting" in {
      val neverStarted = Promise[Source[Envelope, NotUsed]]()
      val sourceProvider = new TestSourceProvider(() => neverStarted.future)
      val running = projectionState(sourceProvider).newRunningInstance()
      val probe = createTestProbe()
      probe.awaitAssert(sourceProvider.sourceCalls.get should ===(1))

      running.stop().futureValue(timeout(5.seconds)) should ===(Done)
    }

    "complete the envelope in flight and save its offset" in {
      val sourceProvider =
        new TestSourceProvider(() => Future.successful(Source.single(Envelope("a", 1L, "msg")).concat(Source.never)))
      val offsetStore = new TestInMemoryOffsetStoreImpl[Long]()
      val processing = Promise[Done]()
      val processed = Promise[Done]()
      val running = projectionState(sourceProvider, offsetStore, _ => {
        processing.trySuccess(Done)
        processed.future
      }).newRunningInstance()
      processing.future.futureValue

      val stopped = running.stop()
      Thread.sleep(500)
      stopped.isCompleted should ===(false)

      processed.success(Done)
      stopped.futureValue(timeout(5.seconds)) should ===(Done)
      offsetStore.lastOffset() should ===(Some(1L))
    }
  }
}
