/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import scala.concurrent.Await
import scala.concurrent.Future

import akka.projection.kafka.GroupOffsets.TopicPartitionKey
import akka.actor.typed.scaladsl.adapter._
import akka.projection.kafka.scaladsl.KafkaSourceProvider
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.common.TopicPartition

class KafkaSourceProviderSpec extends KafkaSpecBase() {
  "KafkaSourceProviderSpec" must {
    "resume from provided offsets" in assertAllStagesStopped {
      val topic = createTopic()
      val groupId = createGroupId()
      val settings = consumerDefaults.withGroupId(groupId)

      Await.result(produce(topic, 1 to 100), remainingOrDefault)

      val provider = KafkaSourceProvider(system.toTyped, settings, Set(topic))
      val readOffsetsHandler =
        () => Future.successful(Option(GroupOffsets(Map(TopicPartitionKey(new TopicPartition(topic, 0)) -> 5L))))
      val probe = Source
        .futureSource(provider.source(readOffsetsHandler))
        .runWith(TestSink.probe)

      probe.request(1)
      val first = probe.expectNext()
      first.offset() shouldBe 5L

      probe.cancel()
    }

    "resume from beginning offsets when none are provided" in assertAllStagesStopped {
      val topic = createTopic()
      val groupId = createGroupId()
      val settings = consumerDefaults.withGroupId(groupId)

      Await.result(produce(topic, 1 to 100), remainingOrDefault)

      val provider = KafkaSourceProvider(system.toTyped, settings, Set(topic))
      val readOffsetsHandler = () => Future.successful(None)
      val probe = Source
        .futureSource(provider.source(readOffsetsHandler))
        .runWith(TestSink.probe)

      probe.request(1)
      val first = probe.expectNext()
      first.offset() shouldBe 0L

      probe.cancel()
    }
  }
}
