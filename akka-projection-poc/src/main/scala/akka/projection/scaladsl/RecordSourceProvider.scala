/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl
import akka.stream.scaladsl.Source

import scala.collection.immutable

class RecordSourceProvider extends SourceProvider[Record, String, Long]{

  private val records = for (i <- 1 to 100) yield Record(i, s"record-$i")
  private val stream = Source(records)

  override def source(offset: Option[Long]): Source[Record, _] = {
    offset match {
      case Some(latestOffset) => stream.drop(latestOffset)
      case _ => stream
    }
  }

  override def extractOffset(envelope: Record): Long = envelope.offset

  override def extractPayload(envelope: Record): String = envelope.payload
}
