/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka

import scala.concurrent.Future

import akka.projection.internal.MergeableOffset
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink

class KafkaSourceProviderSpec extends KafkaSpecBase() {
  "KafkaSourceProviderSpec" must {
    "resume from provided offsets" in assertAllStagesStopped {
      val topic = createTopic()
      val groupId = createGroupId()
      val settings = consumerDefaults.withGroupId(groupId)

      awaitProduce(produce(topic, 1 to 100))

      val provider = KafkaSourceProvider(system, settings, Set(topic))
      val readOffsetsHandler = () => Future.successful(Option(MergeableOffset(Map(s"$topic-0" -> 5L))))
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

      awaitProduce(produce(topic, 1 to 100))

      val provider = KafkaSourceProvider(system, settings, Set(topic))
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
