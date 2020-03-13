/*
 * Copyright (C) 2019-2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.ConsumerMessage.{ CommittableMessage, CommittableOffset }
import akka.projection.scaladsl.EnvelopeExtractor
import org.apache.kafka.clients.consumer.ConsumerRecord

object KafkaExtractors {

  def consumerRecover[K, V] = new ConsumerRecordExtractor[K, V]

  class ConsumerRecordExtractor[K, V] extends EnvelopeExtractor[ConsumerRecord[K, V], V, Long] {
    override def extractOffset(record: ConsumerRecord[K, V]): Long = record.offset()
    override def extractPayload(record: ConsumerRecord[K, V]): V = record.value()

    def exposeRecord: EnvelopeExtractor[ConsumerRecord[K, V], ConsumerRecord[K, V], Long] =
      EnvelopeExtractor.exposeEnvelope(this)

  }

  def committableMessage[K, V] = new CommittableMessageExtractor[K, V]

  class CommittableMessageExtractor[K, V] extends EnvelopeExtractor[CommittableMessage[K, V], V, CommittableOffset] {
    override def extractOffset(envelope: CommittableMessage[K, V]): CommittableOffset = envelope.committableOffset

    override def extractPayload(envelope: CommittableMessage[K, V]): V = envelope.record.value()

    def exposeRecord: EnvelopeExtractor[CommittableMessage[K, V], CommittableMessage[K, V], CommittableOffset] =
      EnvelopeExtractor.exposeEnvelope(this)
  }
}
