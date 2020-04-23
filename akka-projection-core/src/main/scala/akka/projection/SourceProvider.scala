/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection

import akka.stream.scaladsl.Source

trait SourceProvider[Offset, Element] {

  def source(offset: Option[Offset]): Source[Element, _]

  def extractOffset(elt: Element): Offset
}
