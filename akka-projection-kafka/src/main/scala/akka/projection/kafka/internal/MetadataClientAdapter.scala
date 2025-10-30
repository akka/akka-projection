/*
 * Copyright (C) 2020-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.kafka.internal

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.kafka.KafkaConsumerActor
import akka.kafka.scaladsl.MetadataClient
import akka.kafka.ConsumerSettings
import org.apache.kafka.common.TopicPartition

/**
 * INTERNAL API
 */
@InternalApi private[projection] trait MetadataClientAdapter {
  def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]]
  def numPartitions(topics: Set[String]): Future[Int]
  def stop(): Unit
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] object MetadataClientAdapterImpl {
  private val KafkaMetadataTimeout = 10.seconds // FIXME get from config

  private val consumerActorNameCounter = new AtomicInteger
  private def nextConsumerActorName(): String =
    s"kafkaSourceProviderConsumer-${consumerActorNameCounter.incrementAndGet()}"
}

/**
 * INTERNAL API
 */
@InternalApi private[projection] class MetadataClientAdapterImpl(
    system: ActorSystem[_],
    settings: ConsumerSettings[_, _])
    extends MetadataClientAdapter {
  import MetadataClientAdapterImpl._

  private val classic = system.classicSystem.asInstanceOf[ExtendedActorSystem]
  implicit val ec: ExecutionContext = classic.dispatcher

  private lazy val consumerActor = classic.systemActorOf(KafkaConsumerActor.props(settings), nextConsumerActorName())
  private lazy val metadataClient = MetadataClient.create(consumerActor, KafkaMetadataTimeout)

  def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
    metadataClient.getBeginningOffsets(assignedTps)

  def numPartitions(topics: Set[String]): Future[Int] =
    Future.sequence(topics.map(metadataClient.getPartitionsFor)).map(_.map(_.length).sum)

  def stop(): Unit = {
    metadataClient.close()
    classic.stop(consumerActor)
  }
}
