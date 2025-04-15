/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.metrics

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.ProjectionId
import akka.projection.internal.AtLeastOnce
import akka.projection.internal.BacklogStatusProjectionState
import akka.projection.internal.BacklogStatusSourceProvider
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.metrics.tools.InMemInstruments
import akka.projection.internal.metrics.tools.InMemInstrumentsRegistry
import akka.projection.internal.metrics.tools.InternalProjectionStateMetricsSpec
import akka.projection.internal.metrics.tools.TestHandlers
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource

class BacklogStatusTelemetrySpec extends InternalProjectionStateMetricsSpec {
  import BacklogStatusTelemetrySpec._

  "BacklogStatusTelemetry" should {

    "report backlog status" in withTestBacklogStatus() { (sourceProvider, projectionState, instruments) =>
      instruments.backlogStatusInitialCheckIntervalSeconds.set(1) // report every second
      projectionState.start()

      eventually(timeout(3.seconds)) {
        sourceProvider.isRunning shouldBe true
        instruments.startedInvocations.get shouldBe 1
        instruments.backlogStatusCheckIntervalSecondsInvocations.get shouldBe 1
      }

      // no activity yet

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 1
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe 0
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe 0
      }

      val t0 = Instant.now()
      val envelope1 = TestEnvelope("one", TestOffset(t0.plusSeconds(1)))
      val envelope2 = TestEnvelope("two", TestOffset(t0.plusSeconds(2)))

      // first envelope produced by source
      sourceProvider.setLatestEnvelope(envelope1)

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 2
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe envelope1.offset.timestamp.toEpochMilli
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe 0
      }

      // first envelope processed
      projectionState.setLatestOffset(envelope1.offset)

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 3
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe envelope1.offset.timestamp.toEpochMilli
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe envelope1.offset.timestamp.toEpochMilli
      }

      // simulate source provider checked first, second envelope already processed
      projectionState.setLatestOffset(envelope2.offset)

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 4
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe envelope1.offset.timestamp.toEpochMilli
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe envelope2.offset.timestamp.toEpochMilli
      }

      // source provider timestamp is there now
      sourceProvider.setLatestEnvelope(envelope2)

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 5
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe envelope2.offset.timestamp.toEpochMilli
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe envelope2.offset.timestamp.toEpochMilli
      }

      // simulate events having been deleted
      sourceProvider.clearLatestEnvelope()

      eventually(timeout(3.seconds)) {
        instruments.reportTimestampBacklogStatusInvocations.get should be >= 6
        instruments.backlogStatusLatestSourceTimestamp.get shouldBe 0
        instruments.backlogStatusLatestOffsetTimestamp.get shouldBe envelope2.offset.timestamp.toEpochMilli
      }

      sourceProvider.complete()

      eventually(timeout(3.seconds)) {
        instruments.stoppedInvocations.get shouldBe 1
      }
    }

    "not start if check interval is set to zero" in withTestBacklogStatus() {
      (sourceProvider, projectionState, instruments) =>

        projectionState.start()

        eventually(timeout(3.seconds)) {
          sourceProvider.isRunning shouldBe true
          instruments.startedInvocations.get shouldBe 1
          instruments.backlogStatusCheckIntervalSecondsInvocations.get shouldBe 1
          instruments.backlogStatusInitialCheckIntervalSeconds.get shouldBe 0
        }

        // confirm no backlog status reports for 2 seconds
        Thread.sleep(2000)
        instruments.reportTimestampBacklogStatusInvocations.get shouldBe 0

        sourceProvider.complete()

        eventually(timeout(3.seconds)) {
          instruments.stoppedInvocations.get shouldBe 1
        }
    }

    "not start when source provider doesn't support latest event timestamp" in withTestBacklogStatus(
      supportsLatestEventTimestamp = false) { (sourceProvider, projectionState, instruments) =>

      instruments.backlogStatusInitialCheckIntervalSeconds.set(1) // report every second
      projectionState.start()

      eventually(timeout(3.seconds)) {
        sourceProvider.isRunning shouldBe true
        instruments.startedInvocations.get shouldBe 1
      }

      // confirm no backlog status reports for 2 seconds
      Thread.sleep(2000)
      instruments.backlogStatusInitialCheckIntervalSeconds.get shouldBe 1
      instruments.backlogStatusCheckIntervalSecondsInvocations.get shouldBe 0
      instruments.reportTimestampBacklogStatusInvocations.get shouldBe 0

      sourceProvider.complete()

      eventually(timeout(3.seconds)) {
        instruments.stoppedInvocations.get shouldBe 1
      }
    }
  }

  def withTestBacklogStatus(supportsLatestEventTimestamp: Boolean = true)(
      test: (TestBacklogStatusSourceProvider, TestBacklogStatusProjectionState, InMemInstruments) => Unit): Unit = {
    val projectionId = genRandomProjectionId()
    val projectionSettings = ProjectionSettings(system)
    val sourceProvider = new TestBacklogStatusSourceProvider(supportsLatestEventTimestamp)
    val projectionState = new TestBacklogStatusProjectionState(projectionId, sourceProvider, projectionSettings)
    val instruments = InMemInstrumentsRegistry(system).forId(projectionId)
    test(sourceProvider, projectionState, instruments)
  }
}

object BacklogStatusTelemetrySpec {

  final case class TestOffset(timestamp: Instant)

  final case class TestEnvelope(event: String, offset: TestOffset)

  final class TestBacklogStatusSourceProvider(val supportsLatestEventTimestamp: Boolean)(
      implicit system: ActorSystem[_])
      extends SourceProvider[TestOffset, TestEnvelope]
      with BacklogStatusSourceProvider {

    private val latestEnvelope: AtomicReference[Option[TestEnvelope]] = new AtomicReference(None)

    private val sourceProbe = new AtomicReference[TestPublisher.Probe[TestEnvelope]]()

    private val source = TestSource[TestEnvelope]()(system.classicSystem).mapMaterializedValue { probe =>
      sourceProbe.set(probe)
      NotUsed
    }

    def setLatestEnvelope(envelope: TestEnvelope): Unit = latestEnvelope.set(Some(envelope))

    def clearLatestEnvelope(): Unit = latestEnvelope.set(None)

    def isRunning: Boolean = sourceProbe.get ne null

    def complete(): Unit = sourceProbe.get.sendComplete()

    override private[akka] def latestEventTimestamp(): Future[Option[Instant]] =
      Future.successful(latestEnvelope.get.map(_.offset.timestamp))

    override def source(offset: () => Future[Option[TestOffset]]): Future[Source[TestEnvelope, NotUsed]] =
      Future.successful(source)

    override def extractOffset(envelope: TestEnvelope): TestOffset = envelope.offset

    override def extractCreationTime(envelope: TestEnvelope): Long = envelope.offset.timestamp.toEpochMilli
  }

  final class TestBacklogStatusProjectionState(
      projectionId: ProjectionId,
      sourceProvider: TestBacklogStatusSourceProvider,
      settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState(
        projectionId,
        sourceProvider,
        AtLeastOnce(),
        SingleHandlerStrategy(TestHandlers.single),
        NoopStatusObserver,
        settings)
      with BacklogStatusProjectionState {

    private val latestOffset: AtomicReference[Option[TestOffset]] = new AtomicReference(None)

    def setLatestOffset(offset: TestOffset): Unit = latestOffset.set(Some(offset))

    def clearLatestOffset(): Unit = latestOffset.set(None)

    def start(): Unit = mappedSource().run() // doesn't actually process anything

    override private[akka] def latestOffsetTimestamp(): Future[Option[Instant]] =
      Future.successful(latestOffset.get.map(_.timestamp))

    override def logger: LoggingAdapter = Logging(system.classicSystem, classOf[TestBacklogStatusProjectionState])

    override implicit def executionContext: ExecutionContext = system.executionContext

    override def readPaused(): Future[Boolean] = Future.successful(false)

    override def readOffsets(): Future[Option[TestOffset]] = Future.successful(latestOffset.get)

    override def saveOffset(projectionId: ProjectionId, offset: TestOffset): Future[Done] = Future.successful(Done)
  }
}
