/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.scaladsl.Consumer
import akka.kafka.scaladsl.Consumer.Control
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object KafkaSourceProviders {

  def plainSource[K, V](settings: ConsumerSettings[K, V], topicPartition: TopicPartition) =
    new PlainSourceProvider(settings, topicPartition)

  class PlainSourceProvider[K, V](settings: ConsumerSettings[K, V], topicPartition: TopicPartition)
      extends SourceProvider[Long, ConsumerRecord[K, V], Control] {

    override def source(offset: Option[Long]): Source[ConsumerRecord[K, V], Control] =
      Consumer.plainSource(
        settings,
        Subscriptions.assignmentWithOffset(topicPartition -> offset.map(_ + 1).getOrElse(0L)))

  }

}
