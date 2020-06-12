package akka.projection.internal

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.InternalProjectionStateMetricsSpec.Envelope
import akka.projection.internal.InternalProjectionStateMetricsSpec.sourceProvider
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object InternalProjectionStateMetricsSpec {
  def config: Config = ConfigFactory.parseString("""
      akka {
        loglevel = "DEBUG"
      }
      akka.projection {
        telemetry.fqcn = "akka.projection.internal.InMemTelemetry"
      }
      """)
  case class Envelope(id: String, offset: Long, message: String)

  def sourceProvider(
      system: ActorSystem[_],
      id: String,
      complete: Boolean = true,
      verifyOffsetF: Long => OffsetVerification = _ => VerificationSuccess): SourceProvider[Long, Envelope] = {
    val envelopes =
      List(
        Envelope(id, 1L, "abc"),
        Envelope(id, 2L, "def"),
        Envelope(id, 3L, "ghi"),
        Envelope(id, 4L, "jkl"),
        Envelope(id, 5L, "mno"),
        Envelope(id, 6L, "pqr"))

    val src = if (complete) Source(envelopes) else Source(envelopes).concat(Source.maybe)
    TestSourceProvider(system, src, verifyOffsetF)
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
  // test model is as simple as a text that gets other string concatenated to it
  case class ConcatStr(id: String, text: String) {
    def concat(newMsg: String) = copy(text = text + "|" + newMsg)
  }

}

class InternalProjectionStateMetricsSpec
    extends ScalaTestWithActorTestKit(InternalProjectionStateMetricsSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext

  "A metric counting successful offset progress" must {
    " when running on `at-least-once`" must {

      "handle single envelopes (without afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(1), orAfterDuration = Some(scala.concurrent.duration.Duration.Zero)),
          (handler) => SingleHandlerStrategy(handler))
        runInternal(tt.projectionState) {
          withClue("check - all values were concatenated") {
            tt.concatStr shouldBe "abc|def|ghi|jkl|mno|pqr"
          }
          withClue("the success counter reflects all events as processed") {
            tt.inMemTelemetry.successfullyProcessed.get should be(6)
          }
        }
      }

      "handle single envelopes (with afterEnvelops optimization)" in {
        val tt = new TelemetryTester(
          AtLeastOnce(afterEnvelopes = Some(3), orAfterDuration = Some(500.millis)),
          (handler) => SingleHandlerStrategy(handler))

        runInternal(tt.projectionState) {
          withClue("the success counter reflects all events as processed") {
            tt.inMemTelemetry.successfullyProcessed.get should be(6)
            tt.inMemTelemetry.successfullyProcessedInvocations.get should be(2)
          }
        }
      }

      "handle grouped envelopes" in {}
      "handle flows" in {}
    }
    " when running on `exactly-once`" must {
      "handle single envelopes" in {}
      "handle grouped envelopes" in {}
      "handle flows (not used)" ignore {}
    }
    " when running on `at-most-once`" must {
      "handle single envelopes" in {}
      "handle grouped envelopes (not used)" ignore {}
      "handle flows (not used)" ignore {}
    }

    // TODO: use ProjectionTest's sink to control the pace.
  }

  // inspired on ProjectionTestkit's runInternal
  private def runInternal(
      projectionState: InMemInternalProjectionState[_, _],
      max: FiniteDuration = 3.seconds,
      interval: FiniteDuration = 100.millis)(assertFunction: => Unit): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-state-probe")

    val running =
      projectionState.newRunningInstance()
    try {
      probe.awaitAssert(assertFunction, max.dilated, interval)
    } finally {
      Await.result(running.stop(), max)
    }
  }

}

class TelemetryTester(
    val offsetStrategy: OffsetStrategy,
    val handlerStrategyFactory: (Handler[Envelope]) => HandlerStrategy[Envelope])(implicit system: ActorSystem[_]) {
  private def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  private val entityId = UUID.randomUUID().toString
  private val projectionId = genRandomProjectionId()

  val projectionSettings = ProjectionSettings(system)

  // The projectionSettings will only be used as a fallback. The OffsetHandler and
  // the HandlerStrategy arguments take precedence
  var concatStr = ""
  private val handler = new Handler[Envelope] {
    override def process(envelope: Envelope): Future[Done] = synchronized {
      concatStr = if (concatStr == "") envelope.message else s"$concatStr|${envelope.message}"
      Future.successful(Done)
    }
  }
  private val handlerStrategy = handlerStrategyFactory(handler)
  val projectionState =
    new InMemInternalProjectionState[Long, Envelope](
      projectionId,
      sourceProvider(system, entityId),
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

  def newRunningInstance(): RunningProjection = new TestRunningProjection(mappedSource(), killSwitch)

  class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch) extends RunningProjection {

    private val futureDone = source.run()

    override def stop(): Future[Done] = {
      killSwitch.shutdown()
      futureDone
    }
  }
}
class InMemTelemetry(projectionId: ProjectionId, system: ActorSystem[_]) extends Telemetry(projectionId, system) {
  val successfullyProcessed = new AtomicInteger(0)
  val successfullyProcessedInvocations = new AtomicInteger(0)

  override def failed(projectionId: ProjectionId, cause: Throwable): Unit = ???

  override def stopped(projectionId: ProjectionId): Unit = ???

  override def beforeProcess[Offset, Envelope](projectionId: ProjectionId): AnyRef = { null }

  override def afterProcess[Offset, Envelope](projectionId: ProjectionId, telemetryContext: AnyRef): Unit = {}

  override def onEnvelopeSuccess[Offset, Envelope](projectionId: ProjectionId, successCount: Int): Unit = {
    successfullyProcessedInvocations.incrementAndGet()
    successfullyProcessed.addAndGet(successCount)
  }

  override def error[Offset, Envelope](projectionId: ProjectionId, cause: Throwable): Unit = ???
}
