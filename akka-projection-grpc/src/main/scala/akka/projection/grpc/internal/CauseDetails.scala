/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object CauseDetails {

  private val MaxCauseDepth = 5
  // guards against cyclic cause chains
  private val MaxChainLength = 50
  private val MaxCauseLength = 200

  /**
   * Compact single line rendering of the cause chain, for exception messages, since restart logging only includes
   * the message of the outermost exception until verbose logging kicks in.
   */
  def apply(cause: Throwable): String = {
    val chain = Iterator.iterate(cause)(_.getCause).takeWhile(_ ne null).take(MaxChainLength).toList
    // keep the root cause, it usually names the reason
    val bounded = if (chain.size > MaxCauseDepth) chain.take(MaxCauseDepth - 1) :+ chain.last else chain
    val messages = bounded.map(th => (th, firstLine(th.getMessage)))
    val rendered = messages.zip(None +: messages.map(m => Some(m._2))).map {
      case ((th, message), previous) =>
        if (message.isEmpty || previous.exists(_.contains(message))) className(th) // message already shown by wrapper
        else s"${className(th)}: ${truncate(message)}"
    }
    rendered.mkString(", caused by: ")
  }

  private def className(th: Throwable): String = {
    val simpleName = th.getClass.getSimpleName
    if (simpleName.isEmpty) th.getClass.getName else simpleName
  }

  private def firstLine(message: String): String =
    if (message eq null) ""
    else sanitize(message.linesIterator.find(_.trim.nonEmpty).getOrElse("").trim)

  // messages can come from the producer or the network, keep control characters out of the logs
  private def sanitize(message: String): String =
    message.map { c =>
      Character.getType(c) match {
        case Character.CONTROL | Character.LINE_SEPARATOR | Character.PARAGRAPH_SEPARATOR => '?'
        case _                                                                            => c
      }
    }

  private def truncate(message: String): String =
    if (message.length <= MaxCauseLength) message else message.take(MaxCauseLength) + "..."
}
