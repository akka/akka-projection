/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl

import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Envelope, Mat] {

  /**
   * Provides a Source[Envelope, _] starting from the passed offset.
   * When Offset is None, the Source should start from the first element.
   *
   */
  def source(offset: Option[Offset]): Source[Envelope, Mat]

}
