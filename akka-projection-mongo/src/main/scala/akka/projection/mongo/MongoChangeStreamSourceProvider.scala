/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import akka.actor.typed.ActorSystem
import akka.projection.mongo.internal.MongoChangeStreamSourceProviderImpl
import org.mongodb.scala.{ MongoClient, MongoClientSettings }
import org.mongodb.scala.bson.conversions

object MongoChangeStreamSourceProvider {

  /**
   * Create a [[akka.projection.scaladsl.SourceProvider]] that resumes from externally managed offsets
   */
  def apply[K, V](
      system: ActorSystem[_],
      clientSettings: MongoClientSettings,
      pipeline: Seq[conversions.Bson]): MongoChangeStreamSourceProviderImpl =
    new MongoChangeStreamSourceProviderImpl(system, MongoClient(clientSettings), pipeline)
}
