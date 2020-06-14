package akka.projection.internal

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.HandlerRecoveryStrategy
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.RestartFlow
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class InternalProjectionStateMetricsSpec
    extends ScalaTestWithActorTestKit(InternalProjectionStateMetricsSpec.config)
    with AnyWordSpecLike
    with LogCapturing {

  import akka.projection.internal.InternalProjectionStateMetricsSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext
  val zero = scala.concurrent.duration.Duration.Zero

  val maxRetries = 100

  // TODO: use ProjectionTest's sink to control the pace and have more fine grained assertions

  "A metric counting offsets committed" must {
    " when running on `at-least-once`" must {

      " use a singleHandler" must {
        "count offsets (without afterEnvelops optimization)" in {
          val single = Handlers.single
          val tt = new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(1)), SingleHandlerStrategy(single))

          runInternal(tt.projectionState) {
            withClue("check - all values were concatenated") {
              single.concatStr shouldBe "abc|def|ghi|jkl|mno|pqr"
            }
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
            }
          }
        }
        "count offsets (with afterEnvelops optimization)" in {
          val tt =
            new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), SingleHandlerStrategy(Handlers.single))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(2)
            }
          }
        }
        "count offsets only once in case of failure" in {
          val single = Handlers.singleWithFailure(0.5f)
          val tt = new TelemetryTester(
            AtLeastOnce(
              afterEnvelopes = Some(1),
              // using retryAndFail to try to get all message through
              recoveryStrategy = Some(HandlerRecoveryStrategy.retryAndFail(maxRetries, 30.millis))),
            SingleHandlerStrategy(single))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
            }
          }
        }
      }
      " use a groupedHandler" must {
        "count offsets" ignore {
          // TODO: this tests fails. Need changes in prod code: `reportProgress has no info of the group size.
          val tt = new TelemetryTester(
            AtLeastOnce(),
            GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2), orAfterDuration = Some(500.millis)))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(3)
            }
          }
        }
        "count envelopes only once in case of failure" ignore {
          // TODO: fix the happy path first
        }
      }
      " use a flowHandler" must {
        "count offsets" in {
          val tt =
            new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), FlowHandlerStrategy[Envelope](Handlers.flow))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(2)
            }
          }
        }
        "count offsets only once in case of failure" in {
          val flow = Handlers.flowWithFailureAndRetries(0.8f, maxRetries)
          val tt =
            new TelemetryTester(AtLeastOnce(afterEnvelopes = Some(3)), FlowHandlerStrategy[Envelope](flow))
          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(2)
            }
          }
        }
      }
    }
    " when running on `exactly-once`" must {
      " use a singleHandler" must {
        "count offsets" in {
          val tt =
            new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(6)
            }
          }
        }
      }
      " use a groupedHandler" must {
        "count offsets" in {
          val tt =
            new TelemetryTester(ExactlyOnce(), GroupedHandlerStrategy(Handlers.grouped, afterEnvelopes = Some(2)))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(3)
            }
          }
        }
      }
      " use a flowHandler" must {
        "count offsets (UNSUPPORTED)" ignore {}
      }
    }
    " when running on `at-most-once`" must {
      " use a singleHandler" must {
        "count offsets" in {
          val tt =
            new TelemetryTester(ExactlyOnce(), SingleHandlerStrategy(Handlers.single))

          runInternal(tt.projectionState) {
            withClue("the success counter reflects all events as processed") {
              tt.inMemTelemetry.offsetsSuccessfullyCommitted.get should be(6)
              tt.inMemTelemetry.onOffsetStoredInvocations.get should be(6)
            }
          }
        }
      }
      "handle grouped envelopes (UNSUPPORTED)" ignore {}
      "handle flows (UNSUPPORTED)" ignore {}
    }
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

object InternalProjectionStateMetricsSpec {
  def config: Config =
    ConfigFactory.parseString(s"""
      akka {
        loglevel = "DEBUG"
      }
      akka.projection {
        telemetry.fqcn = "${classOf[InMemTelemetry].getName}"
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

  object Handlers {
    trait ConcatHandlers {
      def concatStr = ""
    }
    // The projectionSettings will only be used as a fallback. The OffsetHandler and
    // the HandlerStrategy arguments take precedence
    val single = singleWithFailure()
    def singleWithFailure(successRatio: Float = 1.0f) = new Handler[Envelope] with ConcatHandlers {
      require(successRatio >= 0f && successRatio <= 1.0f, s"successRatio must be [0.0f, 1.0f].")
      override def concatStr = acc
      var acc = ""
      override def process(envelope: Envelope): Future[Done] = {
        if (Random.between(0f, 1f) > successRatio)
          Future.failed(throw new RuntimeException("Oh, no! Handler errored."))
        acc = append(acc, envelope.message)
        Future.successful(Done)
      }
    }

    val grouped = new Handler[Seq[Envelope]] with ConcatHandlers {
      override def concatStr = acc
      var acc = ""
      override def process(envelopes: Seq[Envelope]): Future[Done] = {
        val x = envelopes.map(_.message).mkString("|")
        acc = append(acc, x)
        Future.successful(Done)
      }
    }

    val flow = flowWithFailure()

    def flowWithFailureAndRetries(successRatio: Float = 1.0f, maxRetries: Int) =
      RestartFlow
        .withBackoff(30.millis, 10.millis, 0.1, maxRetries) { () =>
          Handlers.flowWithFailure(0.9f).asFlow
        }
        .asFlowWithContext[Envelope, ProjectionContext, ProjectionContext] { case (e, ctx) => (e, ctx) } {
          case (_, ctx) => ctx
        }
        .map { case (done, _) => done }

    def flowWithFailure(
        successRatio: Float = 1.0f): FlowWithContext[Envelope, ProjectionContext, Done, ProjectionContext, _] = {
      require(successRatio >= 0f && successRatio <= 1.0f, s"successRatio must be [0.0f, 1.0f].")
      var acc = ""
      FlowWithContext[Envelope, ProjectionContext]
        .map { env =>
          if (Random.between(0f, 1f) > successRatio)
            throw new RuntimeException("Oh, no! Handler errored.")
          acc = append(acc, env.message)
          Done
        }
    }

    private def append(acc: String, x: String) =
      if (acc == "") x else s"${acc}|${x}"

  }

  class TelemetryTester(offsetStrategy: OffsetStrategy, handlerStrategy: HandlerStrategy[Envelope])(
      implicit system: ActorSystem[_]) {
    private def genRandomProjectionId() =
      ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

    private val entityId = UUID.randomUUID().toString
    private val projectionId = genRandomProjectionId()

    private val projectionSettings = ProjectionSettings(system)

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
    val afterProcessInvocations = new AtomicInteger(0)
    val offsetsSuccessfullyCommitted = new AtomicInteger(0)
    val onOffsetStoredInvocations = new AtomicInteger(0)
    val errorInvocations = new AtomicInteger(0)
    val lastErrorThrowable = new AtomicReference[Throwable]()

    override def failed(projectionId: ProjectionId, cause: Throwable): Unit = ???

    override def stopped(projectionId: ProjectionId): Unit = ???

    override def beforeProcess(projectionId: ProjectionId): AnyRef = { null }

    override def afterProcess(projectionId: ProjectionId, telemetryContext: AnyRef): Unit = {
      afterProcessInvocations.incrementAndGet()
    }

    override def onOffsetStored(projectionId: ProjectionId, successCount: Int): Unit = {
      onOffsetStoredInvocations.incrementAndGet()
      offsetsSuccessfullyCommitted.addAndGet(successCount)
    }

    override def error(projectionId: ProjectionId, cause: Throwable): Unit = {
      lastErrorThrowable.set(cause)
      errorInvocations.incrementAndGet()
    }
  }

}
