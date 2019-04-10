import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition


object KafkaSourceProvider {

  def plainSource[K, V](topicPartition: TopicPartition,
                        settings: ConsumerSettings[K, V]) = new SourceProvider[Long, ConsumerRecord[K, V]] {
    /**
     * Provides a Source[S, _] starting from the passed offset.
     * When Offset is None, the Source will start from the first element.
     *
     */
    override def source(offset: Option[Long]): Source[ConsumerRecord[K, V], _] = {

      def buildSource(fromOffset: Long) = {
        Consumer.plainSource(
          settings,
          Subscriptions.assignmentWithOffset(topicPartition -> fromOffset)
        )
      }

      offset match {
        case Some(fromOffset) => buildSource(fromOffset)
        case None => buildSource(0)
      }
    }
  }

}
