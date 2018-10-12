/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projections

import akka.NotUsed
import akka.stream.scaladsl.Source


case class Record(offset: Long, message: String)

object Record {

  def sourceFactory(): Source[Record, NotUsed] = {
    Source.fromIterator[Record] { () =>
      (for (i <- 0 to 20) yield Record(i, "abc-" + i)).iterator
    }
  }
}