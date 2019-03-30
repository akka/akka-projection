/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.scaladsl.Source

trait SourceProvider[Envelope, Payload, Offset] {


  /**
   * Provides a Source[S, _] starting from the passed offset.
   * When Offset is None, the Source will start from the first element.
   *
   */
  def source(offset: Option[Offset]): Source[Envelope, _]

  def extractOffset(envelope: Envelope): Offset

  def extractPayload(envelope: Envelope): Payload
  
}

