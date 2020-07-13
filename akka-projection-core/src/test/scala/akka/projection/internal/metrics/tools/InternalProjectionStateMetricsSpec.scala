/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal.metrics.tools

import java.util.UUID
import java.util.concurrent.atomic.AtomicReference

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl._
import akka.actor.typed.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.projection.ProjectionId
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.TestStatusObserver
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.FlowHandlerStrategy
import akka.projection.internal.GroupedHandlerStrategy
import akka.projection.internal.HandlerStrategy
import akka.projection.internal.InternalProjectionState
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.OffsetStrategy
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Source
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Superclass for the MetricsSpec run directly over [[InternalProjectionState]]. This provides a fake
 * `SourceProvider` and a fake projection implementation (not Slick, not Cassandra,...) so it can live
 * on akka-projections.core. That implementation has its own offset store and all.
 *
 * MetricsSpecs should create a [[TelemetryTester]] and run the projection using `runInternal` (see existing
 * `XyzMetricsSpec` for more examples.
 */
abstract class InternalProjectionStateMetricsSpec
    extends ScalaTestWithActorTestKit(InternalProjectionStateMetricsSpec.config)
    with AnyWordSpecLike
    with BeforeAndAfter
    with LogCapturing {

  import InternalProjectionStateMetricsSpec._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  implicit val actorSystem: ActorSystem[Nothing] = testKit.system
  implicit val executionContext: ExecutionContext = testKit.system.executionContext
  val zero = scala.concurrent.duration.Duration.Zero

  protected def genRandomProjectionId() =
    ProjectionId(UUID.randomUUID().toString, UUID.randomUUID().toString)

  val maxRetries = 100

  // inspired on ProjectionTestkit's runInternal
  protected def runInternal(
      projectionState: InMemInternalProjectionState[_, _],
      max: FiniteDuration = 3.seconds,
      interval: FiniteDuration = 100.millis)(assertFunction: => Unit): Unit = {

    val probe = testKit.createTestProbe[Nothing]("internal-projection-state-probe")

    val running: RunningProjection =
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
      // Recover fast to speed up tests.
      akka.projection.restart-backoff{
        min-backoff = 30ms
        max-backoff = 50ms
        random-factor = 0.1
      }
      akka.projection {
        telemetry.implementations = [${classOf[InMemTelemetry].getName}]
      }
      """)
  case class Envelope(id: String, offset: Long, message: String) {
    // some time in the past...
    val creationTimestamp = System.currentTimeMillis() - 1000L
  }

  def sourceProvider(system: ActorSystem[_], id: String, numberOfEnvelopes: Int): SourceProvider[Long, Envelope] = {
    val chars = "abcdefghijklkmnopqrstuvwxyz"
    val envelopes = (1 to numberOfEnvelopes).map { offset =>
      Envelope(id, offset.toLong, chars.charAt((offset - 1) % chars.length).toString)
    }
    TestSourceProvider(system, Source(envelopes))
  }

  case class TestSourceProvider(system: ActorSystem[_], src: Source[Envelope, NotUsed])
      extends SourceProvider[Long, Envelope] {
    implicit val executionContext: ExecutionContext = system.executionContext
    override def source(offset: () => Future[Option[Long]]): Future[Source[Envelope, NotUsed]] =
      offset().map {
        case Some(o) => src.dropWhile(_.offset <= o)
        case _       => src
      }

    override def extractOffset(env: Envelope): Long = env.offset

    override def extractCreationTime(envelope: Envelope): Long = envelope.creationTimestamp
  }

  class TelemetryTester(
      offsetStrategy: OffsetStrategy,
      handlerStrategy: HandlerStrategy,
      numberOfEnvelopes: Int = 6,
      statusObserver: StatusObserver[Envelope] = NoopStatusObserver)(
      implicit system: ActorSystem[_],
      projectionId: ProjectionId) {

    private implicit val exCtx = system.executionContext
    val entityId = UUID.randomUUID().toString

    private val projectionSettings = ProjectionSettings(system)

    val offsetStore = new OffsetStore[Long]()

    val adaptedHandlerStrategy: HandlerStrategy = offsetStrategy match {
      case ExactlyOnce(_) =>
        handlerStrategy match {
          case SingleHandlerStrategy(handlerFactory) => {
            val adaptedHandler = () =>
              new Handler[Envelope] {
                override def process(envelope: Envelope): Future[Done] = handlerFactory().process(envelope).flatMap {
                  _ =>
                    offsetStore.saveOffset(envelope.offset)
                }
              }
            SingleHandlerStrategy(adaptedHandler)
          }
          case GroupedHandlerStrategy(handlerFactory, afterEnvelopes, orAfterDuration) => {
            val adaptedHandler = () =>
              new Handler[immutable.Seq[Envelope]] {
                override def process(envelopes: immutable.Seq[Envelope]): Future[Done] =
                  handlerFactory().process(envelopes).flatMap { _ =>
                    offsetStore.saveOffset(envelopes.last.offset)
                  }
              }
            GroupedHandlerStrategy(adaptedHandler, afterEnvelopes, orAfterDuration)
          }
          case FlowHandlerStrategy(_) => handlerStrategy
        }
      case _ => handlerStrategy
    }

    val projectionState =
      new InMemInternalProjectionState[Long, Envelope](
        projectionId,
        sourceProvider(system, entityId, numberOfEnvelopes),
        offsetStrategy,
        adaptedHandlerStrategy,
        statusObserver,
        projectionSettings,
        offsetStore)

    lazy val inMemTelemetry = projectionState.getTelemetry().asInstanceOf[InMemTelemetry]

  }

  class OffsetStore[Offset]() {
    val _offset = new AtomicReference[Option[Offset]](None)
    def readOffsets(): Future[Option[Offset]] = Future.successful(_offset.get)

    def saveOffset(offset: Offset): Future[Done] = {
      _offset.set(Some(offset))
      Future.successful(Done)
    }
  }

  class InMemInternalProjectionState[Offset, Env](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Env],
      offsetStrategy: OffsetStrategy,
      handlerStrategy: HandlerStrategy,
      statusObserver: StatusObserver[Env],
      settings: ProjectionSettings,
      offsetStore: OffsetStore[Offset])(implicit val system: ActorSystem[_])
      extends InternalProjectionState[Offset, Env](
        projectionId,
        sourceProvider,
        offsetStrategy,
        handlerStrategy,
        statusObserver,
        settings) {
    override def logger: LoggingAdapter = Logging(system.classicSystem, this.getClass)

    override implicit def executionContext: ExecutionContext = system.executionContext

    override def readOffsets(): Future[Option[Offset]] = offsetStore.readOffsets()

    override def saveOffset(projectionId: ProjectionId, offset: Offset): Future[Done] =
      offsetStore.saveOffset(offset)

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
