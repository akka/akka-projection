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
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.scalatest.wordspec.AnyWordSpecLike

object HandlerRecoveryImplSpec {
  final case class Envelope(offset: Long, message: String)

  class FailHandler(recoveryStrategy: HandlerRecoveryStrategy, failOnOffset: Long) extends Handler[Envelope] {
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

    override def onFailure(envelope: Envelope, throwable: Throwable): HandlerRecoveryStrategy = recoveryStrategy
  }

  class TestSourceProvider extends SourceProvider[Long, Envelope] {
    override def source(offset: Option[Long]): Source[Envelope, _] = throw new UnsupportedOperationException

    override def extractOffset(envelope: Envelope): Long = envelope.offset
  }
}

class HandlerRecoveryImplSpec extends ScalaTestWithActorTestKit with AnyWordSpecLike with LogCapturing {
  import HandlerRecoveryImplSpec._

  private val logger = Logging(system.toClassic, getClass)
  private val sourceProvider = new TestSourceProvider
  private val env3 = Envelope(offset = 3, "c")

  "HandlerRecovery" must {
    "fail" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.fail, failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 1
    }

    "skip" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.skip, failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 1
    }

    "retryAndFail 1" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.retryAndFail(1, 10.millis), failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
    }

    "retryAndFail 3" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.retryAndFail(3, 10.millis), failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 4
    }

    "retryAndFail after delay" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.retryAndFail(1, 1.second), failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      // first attempt is immediately
      handler.attempts shouldBe 1
      // retries after delay
      Thread.sleep(100)
      handler.attempts shouldBe 1
      result.failed.futureValue.getClass shouldBe classOf[TestException]
      handler.attempts shouldBe 2
    }

    "retryAndSkip 1" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.retryAndSkip(1, 10.millis), failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 2
    }

    "retryAndSkip 3" in {
      val handler = new FailHandler(HandlerRecoveryStrategy.retryAndSkip(3, 10.millis), failOnOffset = 3)
      val result =
        HandlerRecoveryImpl.applyUserRecovery(handler, env3, sourceProvider, logger, () => handler.process(env3))
      result.futureValue shouldBe Done
      handler.attempts shouldBe 4
    }
  }
}
