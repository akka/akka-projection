/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope] {

  /**
   * Provides a Source[S, _] starting from the passed offset.
   * When Offset is None, the Source will start from the first element.
   *
   */
  def source(offset: Option[Offset]): Source[Envelope, _]


}

object SourceProvider {

  /**
   * This method converts a [[SourceProvider]] into one that will not extract the [[Payload]] and instead
   * will deliver the [[Envelope]] to the [[ProjectionHandler]].
   */
  def exposeEnvelope[Envelope, Payload, Offset](sourceProvider: SourceProvider[Envelope, Payload, Offset]) = {
    new SourceProvider[Envelope, Envelope, Offset] {

      override def source(offset: Option[Offset]): Source[Envelope, _] = sourceProvider.source(offset)

      override def extractOffset(envelope: Envelope): Offset = sourceProvider.extractOffset(envelope)

      override def extractPayload(envelope: Envelope): Envelope = envelope
    }
  }
}