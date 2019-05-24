/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object KafkaSourceProviders {

  def plainSource[K, V](settings: ConsumerSettings[K, V],
                        topicPartition: TopicPartition) = new PlainSourceProvider(settings, topicPartition)


  class PlainSourceProvider[K, V](settings: ConsumerSettings[K, V],
                                  topicPartition: TopicPartition) extends SourceProvider[Long, ConsumerRecord[K, V]] {

    override def source(offset: Option[Long]): Source[ConsumerRecord[K, V], _] =
      Consumer.plainSource(
        settings,
        Subscriptions.assignmentWithOffset(topicPartition -> offset.getOrElse(0L))
      )

  }

  def committableSource[K, V](settings: ConsumerSettings[K, V],
                              subscription: Subscription) = new CommittableSourceProvider(settings, subscription)

  class CommittableSourceProvider[K, V](settings: ConsumerSettings[K, V],
                                        subscription: Subscription) extends SourceProvider[CommittableOffset, CommittableMessage[K, V]] {

    override def source(offset: Option[CommittableOffset]): Source[CommittableMessage[K, V], _] =
      Consumer.committableSource(settings, subscription)

  }

}
