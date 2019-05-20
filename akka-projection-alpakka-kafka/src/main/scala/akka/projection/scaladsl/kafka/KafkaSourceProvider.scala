package akka.projection.scaladsl.kafka

import akka.kafka.ConsumerMessage.{CommittableMessage, CommittableOffset}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscription, Subscriptions}
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object KafkaSourceProvider {

  def plainSource[K, V](topicPartition: TopicPartition,
                        settings: ConsumerSettings[K, V]) = new PlainSourceProvider(topicPartition, settings)


  class PlainSourceProvider[K, V](topicPartition: TopicPartition,
                                  settings: ConsumerSettings[K, V]) extends SourceProvider[Long, ConsumerRecord[K, V]] {

    override def source(offset: Option[Long]): Source[ConsumerRecord[K, V], _] =
      Consumer.plainSource(
        settings,
        Subscriptions.assignmentWithOffset(topicPartition -> offset.getOrElse(0L))
      )

  }

  def committableSource[K, V](subscription: Subscription,
                              settings: ConsumerSettings[K, V]) = new CommittableSourceProvider(subscription, settings)

  class CommittableSourceProvider[K, V](subscription: Subscription,
                                        settings: ConsumerSettings[K, V]) extends SourceProvider[CommittableOffset, CommittableMessage[K, V]] {

    override def source(offset: Option[CommittableOffset]): Source[CommittableMessage[K, V], _] =
      Consumer.committableSource(settings, subscription)

  }

}
