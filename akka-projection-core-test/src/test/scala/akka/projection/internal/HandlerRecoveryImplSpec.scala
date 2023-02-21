/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
import akka.projection.ProjectionId
import akka.projection.TestStatusObserver
import akka.projection.scaladsl.Handler
import org.scalatest.wordspec.AnyWordSpecLike

object HandlerRecoveryImplSpec {
  final case class Envelope(offset: Long, message: String)

  class FailHandler(failOnOffset: Long) extends Handler[Envelope] {
    private val _attempts = new AtomicInteger()
    def attempts: Int = _attempts.get

    override def process(envelope: Envelope): Future[Done] = {
      if (envelope.offset == failOnOffset) {
        _attempts.incrementAndGet()
        throw TestException(s"Fail on $failOnOffset after $attempts attempts")
      } else {
        Future.successful(Done)
      }
    }
  }

}

class HandlerRecoveryImplSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import HandlerRecoveryImplSpec._
  import TestStatusObserver._

  private val logger = Logging(system.toClassic, getClass.asInstanceOf[Class[Any]])
  private val failOnOffset: Long = 3
  private val env3 = Envelope(offset = failOnOffset, "c")
  private val projectionId = ProjectionId("test", "1")
  private val someTestException = TestException("err")
  private val neverCompleted = Promise[Done]().future
  private val telemetry = NoopTelemetry

  "HandlerRecovery" must {
    "fail" in {
      val strategy = HandlerRecoveryStrategy.fail
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, neverCompleted, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 1
      statusProbe.expectMessage(Err(env3, someTestException))
    }

    "skip" in {
      val strategy = HandlerRecoveryStrategy.skip
      val statusProbe = createTestProbe[Status]()
      val onSkipProbe = createTestProbe[Done]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(
          env3,
          failOnOffset,
          failOnOffset,
          neverCompleted,
          () => handler.process(env3),
          onSkip = () => {
            onSkipProbe.ref ! Done
            Future.successful(Done)
          })
      result.futureValue shouldBe Done
      handler.attempts shouldBe 1
      statusProbe.expectMessage(Err(env3, someTestException))
      onSkipProbe.expectMessage(Done)
    }

    "retryAndFail 1" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(1, 20.millis)
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, neverCompleted, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
    }

    "retryAndFail 3" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(3, 20.millis)
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, neverCompleted, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 4
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
    }

    "retryAndFail after delay" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(1, 1.second)
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, neverCompleted, () => handler.process(env3))
      // first attempt is immediately
      handler.attempts shouldBe 1
      // retries after delay
      Thread.sleep(100)
      handler.attempts shouldBe 1
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
    }

    "retryAndSkip 1" in {
      val strategy = HandlerRecoveryStrategy.retryAndSkip(1, 20.millis)
      val statusProbe = createTestProbe[Status]()
      val onSkipProbe = createTestProbe[Done]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(
          env3,
          failOnOffset,
          failOnOffset,
          neverCompleted,
          () => handler.process(env3),
          onSkip = () => {
            onSkipProbe.ref ! Done
            Future.successful(Done)
          })
      result.futureValue shouldBe Done
      handler.attempts shouldBe 2
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectNoMessage(20.millis)
      onSkipProbe.expectMessage(Done)
    }

    "retryAndSkip 3" in {
      val strategy = HandlerRecoveryStrategy.retryAndSkip(3, 20.millis)
      val statusProbe = createTestProbe[Status]()
      val onSkipProbe = createTestProbe[Done]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val result =
        handlerRecovery.applyRecovery(
          env3,
          failOnOffset,
          failOnOffset,
          neverCompleted,
          () => handler.process(env3),
          onSkip = () => {
            onSkipProbe.ref ! Done
            Future.successful(Done)
          })
      result.futureValue shouldBe Done
      handler.attempts shouldBe 4
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectNoMessage(20.millis)
      onSkipProbe.expectMessage(Done)
    }

    "abort retryAndFail before first attempt" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(100, 1.second)
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val abortException = TestException("abort")
      val abort = Future.failed[Done](abortException)
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, abort, () => handler.process(env3))
      result.failed.futureValue shouldBe abortException
      // first attempt is immediately but already aborted
      handler.attempts shouldBe 0
    }

    "abort retryAndFail when retrying" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(100, 1.second)
      val statusProbe = createTestProbe[Status]()
      val statusObserver = new TestStatusObserver[Envelope](statusProbe.ref)
      val handlerRecovery =
        HandlerRecoveryImpl[Long, Envelope](projectionId, strategy, logger, statusObserver, telemetry)
      val handler = new FailHandler(failOnOffset)
      val abort = Promise[Done]()
      val result =
        handlerRecovery.applyRecovery(env3, failOnOffset, failOnOffset, abort.future, () => handler.process(env3))
      // first attempt is immediately
      handler.attempts shouldBe 1
      // retries after delay
      eventually {
        handler.attempts shouldBe 2
      }
      val abortException = TestException("abort")
      abort.failure(abortException)

      result.failed.futureValue shouldBe abortException

      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectMessage(Err(env3, someTestException))
      statusProbe.expectNoMessage(1100.millis)
      handler.attempts shouldBe 2
    }
  }
}
