package akka.projection.internal.metrics

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random
import scala.util.control.NoStackTrace

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.Telemetry
import akka.projection.internal.metrics.InternalProjectionStateMetricsSpec.Envelope
import akka.projection.internal.metrics.InternalProjectionStateMetricsSpec.TelemetryException
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory

// TODO: use ProjectionTest's sink to control the pace and have more fine grained assertions
abstract class InternalProjectionStateMetricsSpec
    extends ScalaTestWithActorTestKit(InternalProjectionStateMetricsSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import InternalProjectionStateMetricsSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext
  val zero = scala.concurrent.duration.Duration.Zero

  val maxRetries = 100

  // inspired on ProjectionTestkit's runInternal
  protected def runInternal(
      projectionState: InMemInternalProjectionState[_, _],
      max: FiniteDuration = 3.seconds,
      interval: FiniteDuration = 100.millis)(assertFunction: => Unit): Unit = {

    InMemInstruments.reset()
    val probe = testKit.createTestProbe[Nothing]("internal-projection-state-probe")

    val running: RunningProjection =
      projectionState.newRunningInstance()
    try {
      probe.awaitAssert(assertFunction, max.dilated, interval)
    } finally {
      Await.result(running.stop(), max)
      InMemInstruments.reset()
    }
  }

}

object InternalProjectionStateMetricsSpec {
  def config: Config =
    ConfigFactory.parseString(s"""
      akka {
        loglevel = "DEBUG"
      }
      // Recover fast to speed up tests.
      akka.projection.restart-backoff{
        min-backoff = 30ms
        max-backoff = 50ms
        random-factor = 0.1
      }
      akka.projection {
        telemetry.fqcn = "${classOf[InMemTelemetry].getName}"
      }
      """)
  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(system: ActorSystem[_], id: String, numberOfEnvelopes: Int): SourceProvider[Long, Envelope] = {
    val chars = "abcdefghijklkmnopqrstuvwxyz"
    val envelopes = (1 to numberOfEnvelopes).map { offset =>
      Envelope(id, offset.toLong, chars.charAt(offset - 1).toString)
    }
    TestSourceProvider(system, Source(envelopes), _ => VerificationSuccess)
  }

  case class TestSourceProvider(
      system: ActorSystem[_],
      src: Source[Envelope, _],
      offsetVerificationF: Long => OffsetVerification)
      extends SourceProvider[Long, Envelope] {
    implicit val executionContext: ExecutionContext = system.executionContext
    override def source(offset: () => Future[Option[Long]]): Future[Source[Envelope, _]] =
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }

    override def extractOffset(env: Envelope): Long = env.offset

    override def verifyOffset(offset: Long): OffsetVerification = offsetVerificationF(offset)
  }

  case object TelemetryException extends RuntimeException("Oh, no! Handler errored.") with NoStackTrace

  class TelemetryTester(
      offsetStrategy: OffsetStrategy,
      handlerStrategy: HandlerStrategy[Envelope],
      numberOfEnvelopes: Int = 6)(implicit system: ActorSystem[_]) {
    private def genRandomProjectionId() =
      ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

    private val entityId = UUID.randomUUID().toString
    private val projectionId = genRandomProjectionId()

    private val projectionSettings = ProjectionSettings(system)

    val projectionState =
      new InMemInternalProjectionState[Long, Envelope](
        projectionId,
        sourceProvider(system, entityId, numberOfEnvelopes),
        offsetStrategy,
        handlerStrategy,
        NoopStatusObserver,
        projectionSettings)

    lazy val inMemTelemetry = projectionState.telemetry.asInstanceOf[InMemTelemetry]

  }

  class InMemInternalProjectionState[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      offsetStrategy: OffsetStrategy,
      handlerStrategy: HandlerStrategy[Envelope],
      statusObserver: StatusObserver[Envelope],
      settings: ProjectionSettings)(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Envelope](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {
    override def logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)

    override implicit def executionContext: ExecutionContext = system.executionContext

    override def readOffsets(): Future[Option[Offset]] = Future.successful(None)

    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] = Future.successful(Done)

    def newRunningInstance(): RunningProjection =
      new TestRunningProjection(RunningProjection.withBackoff(() => mappedSource(), settings), killSwitch)

    class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch) extends RunningProjection {

      private val futureDone = source.run()

      override def stop(): Future[Done] = {
        killSwitch.shutdown()
        futureDone
      }
    }
  }

}

object Handlers {
  val single = singleWithFailure()
  def singleWithFailure(successRatio: Float = 1.0f) = new Handler[Envelope] {
    override def process(envelope: Envelope): Future[Done] = {
      flakyProcessor(successRatio)
      Future.successful(Done)
    }
  }

  val grouped = groupedWithFailures()
  def groupedWithFailures(successRatio: Float = 1.0f) = new Handler[Seq[Envelope]] {
    override def process(envelopes: Seq[Envelope]): Future[Done] = {
      flakyProcessor(successRatio)
      Future.successful(Done)
    }
  }

  val flow = flowWithFailure()
  def flowWithFailure(successRatio: Float = 1.0f) =
    FlowWithContext[Envelope, ProjectionContext]
      .map { _ =>
        flakyProcessor(successRatio)
        Done
      }

  private def flakyProcessor(successRatio: Float): Unit = {
    require(successRatio >= 0f && successRatio <= 1.0f, s"successRatio must be [0.0f, 1.0f].")
    if (Random.between(0f, 1f) > successRatio)
      throw TelemetryException
  }
}

object InMemInstruments {
  // the instruments outlive the InMemTelemetry instances. Multiple instances of InMemTelemetry
  // will share these instruments.
  val afterProcessInvocations = new AtomicInteger(0)
  val lastServiceTimeInNanos = new AtomicLong()
  val offsetsSuccessfullyCommitted = new AtomicInteger(0)
  val onOffsetStoredInvocations = new AtomicInteger(0)
  val errorInvocations = new AtomicInteger(0)
  val lastErrorThrowable = new AtomicReference[Throwable](null)

  def reset(): Unit = {
    afterProcessInvocations.set(0)
    lastServiceTimeInNanos.set(0L)
    offsetsSuccessfullyCommitted.set(0)
    onOffsetStoredInvocations.set(0)
    errorInvocations.set(0)
    lastErrorThrowable.set(null)
  }
}

class InMemTelemetry(projectionId: ProjectionId, system: ActorSystem[_]) extends Telemetry(projectionId, system) {
  import InMemInstruments._
  override def failed(cause: Throwable): Unit = ???

  override def stopped(): Unit = ???

  override private[projection] def afterProcess(serviceTimeInNanos: => Long): Unit = {
    afterProcessInvocations.incrementAndGet()
    lastServiceTimeInNanos.set(serviceTimeInNanos)
  }

  override def onOffsetStored(successCount: Int): Unit = {
    onOffsetStoredInvocations.incrementAndGet()
    offsetsSuccessfullyCommitted.addAndGet(successCount)
  }

  override def error(cause: Throwable): Unit = {
    lastErrorThrowable.set(cause)
    errorInvocations.incrementAndGet()
  }

}
