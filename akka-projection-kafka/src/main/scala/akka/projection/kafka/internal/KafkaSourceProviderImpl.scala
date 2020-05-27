/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.actor.ClassicActorSystemProvider
import akka.actor.ExtendedActorSystem
import akka.annotation.InternalApi
import akka.kafka.ConsumerSettings
import akka.kafka.KafkaConsumerActor
import akka.kafka.RestrictedConsumer
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.MetadataClient
import akka.kafka.scaladsl.PartitionAssignmentHandler
import akka.projection.OffsetVerification
import akka.projection.Success
import akka.projection.SkipOffset
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
  private type ReadOffsets = () => Future[Option[GroupOffsets]]

  private val EmptyTps: Set[TopicPartition] = Set.empty
  private val KafkaMetadataTimeout = 10.seconds // TODO: get from config

  private val consumerActorNameCounter = new AtomicInteger
  private def nextConsumerActorName(): String =
    s"kafkaSourceProviderConsumer-${consumerActorNameCounter.incrementAndGet()}"
}

/**
 * INTERNAL API
 */
@InternalApi private[akka] class KafkaSourceProviderImpl[K, V](
    systemProvider: ClassicActorSystemProvider,
    settings: ConsumerSettings[K, V],
    topics: Set[String])
    extends SourceProvider[GroupOffsets, ConsumerRecord[K, V]] {
  import KafkaSourceProviderImpl._

  private val system = systemProvider.classicSystem.asInstanceOf[ExtendedActorSystem]
  private implicit val dispatcher: ExecutionContext = systemProvider.classicSystem.dispatcher

  private val subscription = Subscriptions.topics(topics).withPartitionAssignmentHandler(new ProjectionPartitionHandler)
  private lazy val consumerActor = system.systemActorOf(KafkaConsumerActor.props(settings), nextConsumerActorName())
  private lazy val metadataClient = MetadataClient.create(consumerActor, KafkaMetadataTimeout)(dispatcher)
  private lazy val assignedPartitions = new AtomicReference[Set[TopicPartition]](EmptyTps)

  private def stopMetadataClient(): Unit = {
    metadataClient.close()
    system.stop(consumerActor)
  }

  override def source(readOffsets: ReadOffsets): Future[Source[ConsumerRecord[K, V], _]] = {
    // get the total number of partitions to configure the `breadth` parameter, or we could just use a really large
    // number.  i don't think using a large number would present a problem.
    val numPartitionsF = Future.sequence(topics.map(metadataClient.getPartitionsFor)).map(_.map(_.length).sum)
    numPartitionsF.failed.foreach(_ => stopMetadataClient())
    numPartitionsF.map { numPartitions =>
      Consumer
        .plainPartitionedManualOffsetSource(settings, subscription, getOffsetsOnAssign(readOffsets))
        .flatMapMerge(numPartitions, {
          case (_, partitionedSource) => partitionedSource
        })
        .watchTermination()(Keep.right)
        .mapMaterializedValue { terminated =>
          terminated.onComplete(_ => stopMetadataClient())
        }
    }
  }

  override def extractOffset(record: ConsumerRecord[K, V]): GroupOffsets = GroupOffsets(record)

  override def verifyOffset(offsets: GroupOffsets): OffsetVerification = {
    if ((assignedPartitions.get() -- offsets.partitions) == EmptyTps)
      SkipOffset("The offset contains Kafka topic partitions that were revoked or lost in a previous rebalance")
    else
      Success
  }

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

  private class ProjectionPartitionHandler extends PartitionAssignmentHandler {
    override def onRevoke(revokedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      removeFromAssigned(revokedTps)

    override def onAssign(assignedTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions.set(assignedTps)

    override def onLost(lostTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      removeFromAssigned(lostTps)

    override def onStop(currentTps: Set[TopicPartition], consumer: RestrictedConsumer): Unit =
      assignedPartitions.set(EmptyTps)

    private def removeFromAssigned(revokedTps: Set[TopicPartition]): Set[TopicPartition] =
      assignedPartitions.accumulateAndGet(revokedTps, (assigned, revoked) => assigned.diff(revoked))
  }
}
