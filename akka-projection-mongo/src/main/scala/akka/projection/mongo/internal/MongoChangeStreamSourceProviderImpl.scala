/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo.internal

import akka.NotUsed

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.projection.mongo.Offset
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.mongodb.scala.bson.conversions
import org.mongodb.scala.{ Document, MongoClient }
import org.mongodb.scala.model.changestream.ChangeStreamDocument
import reactivestreams.Implicits._

/**
 * INTERNAL API
 */
@InternalApi private[akka] object MongoChangeStreamSourceProviderImpl {
  private[mongo] type ReadOffsets = () => Future[Option[Offset]]
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class MongoChangeStreamSourceProviderImpl(
    system: ActorSystem[_],
    mongoClient: MongoClient,
    pipeline: Seq[conversions.Bson])
    extends SourceProvider[Offset, ChangeStreamDocument[Document]] {
  import MongoChangeStreamSourceProviderImpl._

  private implicit val executionContext: ExecutionContext = system.executionContext

  override def source(readOffsets: ReadOffsets): Future[Source[ChangeStreamDocument[Document], NotUsed]] = {
    readOffsets()
      .map {
        case Some(value) =>
          mongoClient.watch[Document](pipeline).resumeAfter(value.doc)
        case None =>
          mongoClient.watch[Document](pipeline)
      }
      .map(Source.fromPublisher(_))

  }

  override def extractOffset(record: ChangeStreamDocument[Document]): Offset = Offset(record)

}
