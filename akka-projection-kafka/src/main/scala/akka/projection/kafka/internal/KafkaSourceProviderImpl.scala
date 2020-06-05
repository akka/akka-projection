/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.kafka.ConsumerSettings
import akka.kafka.RestrictedConsumer
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.projection.OffsetVerification
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.kafka.GroupOffsets
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

/**
 * INTERNAL API
 */
@InternalApi private[akka] object KafkaSourceProviderImpl {
  private[kafka] type ReadOffsets = () => Future[Option[GroupOffsets]]
  private val EmptyTps: Set[TopicPartition] = Set.empty
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class KafkaSourceProviderImpl[K, V](
    system: ActorSystem[_],
    settings: ConsumerSettings[K, V],
    topics: Set[String],
    metadataClient: MetadataClientAdapter)
    extends SourceProvider[GroupOffsets, ConsumerRecord[K, V]] {
  import KafkaSourceProviderImpl._

  private implicit val executionContext: ExecutionContext = system.executionContext

  private[kafka] val partitionHandler = new ProjectionPartitionHandler
  private val subscription = Subscriptions.topics(topics).withPartitionAssignmentHandler(partitionHandler)
  // assigned partitions is only ever mutated by consumer rebalance partition handler executed in the Kafka consumer
  // poll thread in the Alpakka Kafka `KafkaConsumerActor`
  @volatile private var assignedPartitions: Set[TopicPartition] = Set.empty

  protected[internal] def _source(
      readOffsets: ReadOffsets,
      numPartitions: Int): Source[ConsumerRecord[K, V], Consumer.Control] =
    Consumer
      .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign(readOffsets))
      .flatMapMerge(numPartitions, {
        case (_, partitionedSource) => partitionedSource
      })

  override def source(readOffsets: ReadOffsets): Future[Source[ConsumerRecord[K, V], _]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
    val numPartitionsF = metadataClient.numPartitions(topics)
    numPartitionsF.failed.foreach(_ => metadataClient.stop())
    numPartitionsF.map { numPartitions =>
      _source(readOffsets, numPartitions)
        .watchTermination()(Keep.right)
        .mapMaterializedValue { terminated =>
          terminated.onComplete(_ => metadataClient.stop())
        }
    }
  }

  override def extractOffset(record: ConsumerRecord[K, V]): GroupOffsets = GroupOffsets(record)

  override def verifyOffset(offsets: GroupOffsets): OffsetVerification = {
    if (offsets.partitions.forall(assignedPartitions.contains))
      VerificationSuccess
    else
      VerificationFailure(
        "The offset contains Kafka topic partitions that were revoked or lost in a previous rebalance")
  }

  override def isOffsetMergeable: Boolean = true

  private def getOffsetsOnAssign(readOffsets: ReadOffsets): Set[TopicPartition] => Future[Map[TopicPartition, Long]] =
    (assignedTps: Set[TopicPartition]) =>
      readOffsets()
        .flatMap {
          case Some(groupOffsets) =>
            Future.successful(groupOffsets.entries.flatMap {
              case (topicPartitionKey, offset) =>
                val tp = topicPartitionKey.tp
                if (assignedTps.contains(tp)) Map(tp -> offset)
                else Map.empty
            })
          case None => metadataClient.getBeginningOffsets(assignedTps)
        }
        .recover {
          case ex => throw new RuntimeException("External offsets could not be retrieved", ex)
        }

  private[kafka] class ProjectionPartitionHandler extends PartitionAssignmentHandler {
    override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions = assignedPartitions.diff(revokedTps)

    override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions = assignedTps

    override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions = assignedPartitions.diff(lostTps)

    override def onStop(currentTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions = EmptyTps
  }
}
