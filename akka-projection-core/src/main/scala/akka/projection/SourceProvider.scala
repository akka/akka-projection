/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: Option[Offset]): Source[Envelope, _]

  def extractOffset(envelope: Envelope): Offset
}
