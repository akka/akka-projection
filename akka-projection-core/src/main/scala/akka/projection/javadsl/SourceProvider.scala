/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional

import akka.stream.javadsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: Optional[Offset]): Source[Envelope, _]

  def extractOffset(envelope: Envelope): Offset

}
