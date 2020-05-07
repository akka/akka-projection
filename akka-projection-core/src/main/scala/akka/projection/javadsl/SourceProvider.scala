/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.javadsl

import java.util.Optional
import java.util.concurrent.CompletionStage

import akka.stream.javadsl.Source

trait SourceProvider[Offset, Envelope] {

  def source(offset: () => CompletionStage[Optional[Offset]]): CompletionStage[Source[Envelope, _]]

  def extractOffset(envelope: Envelope): Offset
}
