/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.NotUsed
import akka.stream.scaladsl.Source

trait SourceProvider[E] {
  def get(): Source[E, NotUsed]
}
