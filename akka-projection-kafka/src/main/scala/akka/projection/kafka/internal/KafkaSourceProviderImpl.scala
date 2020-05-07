package akka.projection.kafka.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.control.NonFatal

import akka.actor.ClassicActorSystemProvider
import akka.annotation.InternalApi
import akka.kafka.ConsumerSettings
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.MetadataClient
import akka.projection.internal.MergeableOffset
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KafkaSourceProviderImpl {
  private type ReadOffsets = () => Future[Option[MergeableOffset[Long]]]
  private val RegexTp = """(.+)-(\d+)""".r
  private val KafkaMetadataTimeout = 10.seconds // TODO: get from config
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class KafkaSourceProviderImpl[K, V](
    systemProvider: ClassicActorSystemProvider,
    settings: ConsumerSettings[K, V],
    topics: Set[String])
    extends SourceProvider[MergeableOffset[Long], ConsumerRecord[K, V]] {
  import KafkaSourceProviderImpl._

  implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

  private val subscription = Subscriptions.topics(topics)
  private val client = MetadataClient.create(settings, KafkaMetadataTimeout)(systemProvider.classicSystem, dispatcher)

  override def source(readOffsets: ReadOffsets): Future[Source[ConsumerRecord[K, V], _]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
    val numPartitionsF = Future.sequence(topics.map(client.getPartitionsFor)).map(_.map(_.length).sum)
    numPartitionsF.map { numPartitions =>
      Consumer
        .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign(readOffsets))
        .flatMapMerge(numPartitions, {
          case (_, partitionedSource) => partitionedSource
        })
    }
  }

  override def extractOffset(envelope: ConsumerRecord[K, V]): MergeableOffset[Long] = {
    val key = envelope.topic() + "-" + envelope.partition()
    MergeableOffset(Map(key -> envelope.offset()))
  }

  private def getOffsetsOnAssign(readOffsets: ReadOffsets): Set[TopicPartition] => Future[Map[TopicPartition, Long]] =
    (assignedTps: Set[TopicPartition]) =>
      readOffsets()
        .flatMap {
          case Some(mergeableOffsets) =>
            Future.successful(mergeableOffsets.entries.flatMap {
              case (surrogateProjectionKey, offset) =>
                val tp = parseProjectionKey(surrogateProjectionKey)
                if (assignedTps.contains(tp)) Map(tp -> offset)
                else Map.empty
            })
          case None => client.getBeginningOffsets(assignedTps)
        }
        .recover {
          case NonFatal(ex) => throw new RuntimeException("External offsets could not be retrieved", ex)
        }

  private def parseProjectionKey(surrogateProjectionKey: String): TopicPartition = {
    surrogateProjectionKey match {
      case RegexTp(topic, partition) => new TopicPartition(topic, partition.toInt)
      case _ =>
        throw new IllegalArgumentException(
          s"Row entry name (${surrogateProjectionKey}) must match pattern: ${RegexTp.pattern.toString}")
    }
  }
}
