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

//  private[mongo] val partitionHandler = new ProjectionPartitionHandler
//  private val subscription = Subscriptions.topics(topics).withPartitionAssignmentHandler(partitionHandler)
//  // assigned partitions is only ever mutated by consumer rebalance partition handler executed in the Kafka consumer
//  // poll thread in the Alpakka Kafka `KafkaConsumerActor`
//  @volatile private var assignedPartitions: Set[TopicPartition] = Set.empty

//  protected[internal] def _source(
//      readOffsets: ReadOffsets,
//      numPartitions: Int): Source[ConsumerRecord[K, V], Consumer.Control] =
//    Consumer
//      .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign(readOffsets))
//      .flatMapMerge(numPartitions, {
//        case (_, partitionedSource) => partitionedSource
//      })

  override def source(readOffsets: ReadOffsets): Future[Source[ChangeStreamDocument[Document], NotUsed]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
//    val numPartitionsF = metadataClient.numPartitions(topics)
//    numPartitionsF.failed.foreach(_ => metadataClient.stop())
//    numPartitionsF.map { numPartitions =>
//      _source(readOffsets, numPartitions)
//        .watchTermination()(Keep.right)
//        .mapMaterializedValue { terminated =>
//          terminated.onComplete(_ => metadataClient.stop())
//        }
//    }
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
