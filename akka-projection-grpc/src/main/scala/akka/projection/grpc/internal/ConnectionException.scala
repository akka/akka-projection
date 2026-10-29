/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.internal

import akka.annotation.InternalApi

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ConnectionException {

  private val MaxCauseDepth = 5
  private val MaxCauseLength = 200

  /**
   * Compact single line rendering of the cause chain, for exception messages, since restart logging only includes
   * the message of the outermost exception until verbose logging kicks in.
   */
  def causeDetails(cause: Throwable): String = {
    val chain = Iterator.iterate(cause)(_.getCause).takeWhile(_ ne null).take(MaxCauseDepth).toList
    // causes are often already included in the message of the wrapping exception, skip those
    val messages = chain.map(th => (th, firstLine(th.getMessage))).foldLeft(List.empty[(Throwable, String)]) {
      case (acc @ (_, previous) :: _, (_, message)) if message.nonEmpty && previous.contains(message) => acc
      case (acc, entry)                                                                               => entry :: acc
    }
    messages.reverse
      .map {
        case (th, "")      => className(th)
        case (th, message) => s"${className(th)}: ${truncate(message)}"
      }
      .mkString(", caused by: ")
  }

  private def className(th: Throwable): String = {
    val simpleName = th.getClass.getSimpleName
    if (simpleName.isEmpty) th.getClass.getName else simpleName
  }

  private def firstLine(message: String): String =
    if (message eq null) "" else message.linesIterator.find(_.trim.nonEmpty).getOrElse("").trim

  private def truncate(message: String): String =
    if (message.length <= MaxCauseLength) message else message.take(MaxCauseLength) + "..."
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ConnectionException(host: String, port: String, streamId: String, cause: Throwable)
    extends RuntimeException(
      s"Connection to $host:$port for stream id $streamId failed or lost, will be retried: " +
      ConnectionException.causeDetails(cause),
      cause)
