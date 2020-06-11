/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.model.changestream.ChangeStreamDocument

object Offset {
  def apply(doc: ChangeStreamDocument[_]): Offset = {
    new Offset(doc.getResumeToken)
  }
}

class Offset private (val doc: BsonDocument)
