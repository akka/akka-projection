/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import scala.concurrent.duration.FiniteDuration

import akka.annotation.InternalApi
import akka.util.JavaDurationConverters._

trait HandlerRecovery[Envelope] {

  /**
   * Override this to define the error handler strategy when processing an `Envelope` fails.
   * By default it will immediately give up and fail the stream.
   */
  def onFailure(envelope: Envelope, throwable: Throwable): HandlerRecoveryStrategy = {
    HandlerRecoveryStrategy.fail
  }
}

/**
 * Error handling strategy for when processing an `Envelope` fails. Defined in the `Handler` by
 * overriding [[HandlerRecovery.onFailure]].
 */
sealed trait HandlerRecoveryStrategy

object HandlerRecoveryStrategy {
  import Internal._

  /**
   * If the first attempt to invoke the handler fails it will immediately give up
   * and fail the stream.
   */
  def fail: HandlerRecoveryStrategy = Fail

  /**
   * If the first attempt to invoke the handler fails it will immediately give up,
   * discard the element and continue with next.
   */
  def skip: HandlerRecoveryStrategy = Skip

  /**
   * Scala API: If the first attempt to invoke the handler fails it will retry invoking the handler with the
   * same envelope this number of `retries` with the `delay` between each attempt. It will give up
   * and fail the stream if all attempts fail.
   */
  def retryAndFail(retries: Int, delay: FiniteDuration): HandlerRecoveryStrategy =
    if (retries < 1) fail else RetryAndFail(retries, delay)

  /**
   * Java API: If the first attempt to invoke the handler fails it will retry invoking the handler with the
   * same envelope this number of `retries` with the `delay` between each attempt. It will give up
   * and fail the stream if all attempts fail.
   */
  def retryAndFail(retries: Int, delay: java.time.Duration): HandlerRecoveryStrategy =
    retryAndFail(retries, delay.asScala)

  /**
   * Scala API: If the first attempt to invoke the handler fails it will retry invoking the handler with the
   * same envelope this number of `retries` with the `delay` between each attempt. It will give up,
   * discard the element and continue with next if all attempts fail.
   */
  def retryAndSkip(retries: Int, delay: FiniteDuration): HandlerRecoveryStrategy =
    if (retries < 1) fail else RetryAndSkip(retries, delay)

  /**
   * Java API: If the first attempt to invoke the handler fails it will retry invoking the handler with the
   * same envelope this number of `retries` with the `delay` between each attempt. It will give up,
   * discard the element and continue with next if all attempts fail.
   */
  def retryAndSkip(retries: Int, delay: java.time.Duration): HandlerRecoveryStrategy =
    retryAndSkip(retries, delay.asScala)

  /**
   * INTERNAL API: placed here instead of the `internal` package because of sealed trait
   */
  @InternalApi private[akka] object Internal {
    case object Fail extends HandlerRecoveryStrategy
    case object Skip extends HandlerRecoveryStrategy
    final case class RetryAndFail(retries: Int, delay: FiniteDuration) extends HandlerRecoveryStrategy {
      require(retries > 0, "retries must be > 0")
    }
    final case class RetryAndSkip(retries: Int, delay: FiniteDuration) extends HandlerRecoveryStrategy {
      require(retries > 0, "retries must be > 0")
    }
  }
}
