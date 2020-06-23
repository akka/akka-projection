/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import java.util.Optional
import java.util.concurrent.CompletionStage
import java.util.function.Supplier

import scala.compat.java8.FutureConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.NotUsed
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
import akka.projection.javadsl
import akka.projection.kafka.GroupOffsets
import akka.projection.scaladsl
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType

/**
 * INTERNAL API
 */
@InternalApi private[projection] object KafkaSourceProviderImpl {
  private[kafka] type ReadOffsets = () => Future[Option[GroupOffsets]]
  private val EmptyTps: Set[TopicPartition] = Set.empty
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class KafkaSourceProviderImpl[K, V](
    system: ActorSystem[_],
    settings: ConsumerSettings[K, V],
    topics: Set[String],
    metadataClient: MetadataClientAdapter,
    sourceProviderSettings: KafkaSourceProviderSettings)
    extends javadsl.SourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.SourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with javadsl.VerifiableSourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.VerifiableSourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with javadsl.MergeableOffsetSourceProvider[GroupOffsets.TopicPartitionKey, GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.MergeableOffsetSourceProvider[GroupOffsets.TopicPartitionKey, GroupOffsets, ConsumerRecord[K, V]]
    with javadsl.CreationTimeSourceProvider[GroupOffsets, ConsumerRecord[K, V]]
    with scaladsl.CreationTimeSourceProvider[GroupOffsets, ConsumerRecord[K, V]] {

  import KafkaSourceProviderImpl._

  private implicit val executionContext: ExecutionContext = system.executionContext

  private[kafka] val partitionHandler = new ProjectionPartitionHandler
  private val scheduler = system.classicSystem.scheduler
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

  override def source(readOffsets: ReadOffsets): Future[Source[ConsumerRecord[K, V], NotUsed]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
    val numPartitionsF = metadataClient.numPartitions(topics)
    numPartitionsF.failed.foreach(_ => metadataClient.stop())
    numPartitionsF.map { numPartitions =>
      _source(readOffsets, numPartitions)
        .watchTermination()(Keep.right)
        .mapMaterializedValue { terminated =>
          terminated.onComplete(_ => metadataClient.stop())
          NotUsed
        }
    }
  }

  override def source(readOffsets: Supplier[CompletionStage[Optional[GroupOffsets]]])
      : CompletionStage[akka.stream.javadsl.Source[ConsumerRecord[K, V], NotUsed]] = {
    source(() => readOffsets.get().toScala.map(_.asScala)).map(_.asJava).toJava
  }

  override def extractOffset(record: ConsumerRecord[K, V]): GroupOffsets = GroupOffsets(record)

  override def verifyOffset(offsets: GroupOffsets): OffsetVerification = {
    if (offsets.partitions.forall(assignedPartitions.contains))
      VerificationSuccess
    else
      VerificationFailure(
        "The offset contains Kafka topic partitions that were revoked or lost in a previous rebalance")
  }

  override def extractCreationTime(record: ConsumerRecord[K, V]): Long = {
    if (record.timestampType() == TimestampType.CREATE_TIME)
      record.timestamp()
    else
      0L
  }

  private def getOffsetsOnAssign(readOffsets: ReadOffsets): Set[TopicPartition] => Future[Map[TopicPartition, Long]] =
    (assignedTps: Set[TopicPartition]) => {
      val delay = sourceProviderSettings.readOffsetDelay
      akka.pattern.after(delay, scheduler) {
        readOffsets()
          .flatMap {
            case Some(groupOffsets) =>
              val filteredMap = groupOffsets.entries.collect {
                case (topicPartitionKey, offset) if assignedTps.contains(topicPartitionKey.tp) =>
                  (topicPartitionKey.tp -> offset)
              }
              Future.successful(filteredMap)
            case None => metadataClient.getBeginningOffsets(assignedTps)
          }
          .recover {
            case ex => throw new RuntimeException("External offsets could not be retrieved", ex)
          }
      }
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
