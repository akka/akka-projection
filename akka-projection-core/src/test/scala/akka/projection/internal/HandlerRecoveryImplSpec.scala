/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.TestException
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.scaladsl.adapter._
import akka.event.Logging
import akka.projection.HandlerRecoveryStrategy
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

  private val logger = Logging(system.toClassic, getClass)
  private val failOnOffset = 3
  private val env3 = Envelope(offset = failOnOffset, "c")

  "HandlerRecovery" must {
    "fail" in {
      val strategy = HandlerRecoveryStrategy.fail
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 1
    }

    "skip" in {
      val strategy = HandlerRecoveryStrategy.skip
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 1
    }

    "retryAndFail 1" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(1, 20.millis)
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
    }

    "retryAndFail 3" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(3, 20.millis)
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 4
    }

    "retryAndFail after delay" in {
      val strategy = HandlerRecoveryStrategy.retryAndFail(1, 1.second)
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      // first attempt is immediately
      handler.attempts shouldBe 1
      // retries after delay
      Thread.sleep(100)
      handler.attempts shouldBe 1
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
    }

    "retryAndSkip 1" in {
      val strategy = HandlerRecoveryStrategy.retryAndSkip(1, 20.millis)
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 2
    }

    "retryAndSkip 3" in {
      val strategy = HandlerRecoveryStrategy.retryAndSkip(3, 20.millis)
      val handler = new FailHandler(failOnOffset)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(strategy, failOnOffset, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 4
    }
  }
}
