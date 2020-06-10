/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import akka.projection.MergeableKey
import akka.projection.MergeableOffset
import akka.projection.kafka.GroupOffsets.TopicPartitionKey
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

object GroupOffsets {
  private val RegexTp = """(.+)-(\d+)""".r

  def apply(record: ConsumerRecord[_, _]): GroupOffsets = {
    val key = TopicPartitionKey(new TopicPartition(record.topic(), record.partition()))
    new GroupOffsets(Map(key -> record.offset()))
  }

  private[kafka] def apply(offsets: Map[TopicPartitionKey, Long]) = new GroupOffsets(offsets)

  def partitionToKey(tp: TopicPartition): String = tp.topic() + "-" + tp.partition()
  def keyToPartition(key: String): TopicPartition = key match {
    case RegexTp(topic, partition) => new TopicPartition(topic, partition.toInt)
    case _ =>
      throw new IllegalArgumentException(s"Row entry name ($key) must match pattern: ${RegexTp.pattern.toString}")
  }

  final case class TopicPartitionKey(tp: TopicPartition) extends MergeableKey {
    override def surrogateKey: String = GroupOffsets.partitionToKey(tp)
  }
}

class GroupOffsets private (offsets: Map[TopicPartitionKey, Long])
    extends MergeableOffset[TopicPartitionKey, Long](offsets) {
  lazy val partitions: Set[TopicPartition] = offsets.keys.map(_.tp).toSet
}
