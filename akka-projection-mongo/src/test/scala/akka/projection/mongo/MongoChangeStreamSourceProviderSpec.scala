/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.mongo

import scala.concurrent.{ Await, Future }
import akka.actor.typed.scaladsl.adapter._
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink
import org.mongodb.scala.{ Completed, Document, MongoClient }

class MongoChangeStreamSourceProviderSpec extends MongoSpecBase() {

  def produce(range: Seq[Int]): Future[Completed] = {
    MongoClient(clientSettings)
      .getDatabase("test")
      .getCollection("test")
      .insertMany(range.map(i => Document("val" -> i)))
      .toFuture()
  }

  "KafkaSourceProviderSpec" must {
//    "resume from provided offsets" in assertAllStagesStopped {
//      val topic = createTopic()
//      val groupId = createGroupId()
//      val settings = consumerDefaults.withGroupId(groupId)
//
//      Await.result(produce(topic, 1 to 100), remainingOrDefault)
//
//      val provider = MongoChangeStreamSourceProvider(system.toTyped, Seq.empty)
//      val readOffsetsHandler =
//        () => Future.successful(Option(Offset(Map(TopicPartitionKey(new TopicPartition(topic, 0)) -> 5L))))
//      val probe = Source
//        .futureSource(provider.source(readOffsetsHandler))
//        .runWith(TestSink.probe)
//
//      probe.request(1)
//      val first = probe.expectNext()
//      first.offset() shouldBe 5L
//
//      probe.cancel()
//    }

    "resume from beginning offsets when none are provided" in assertAllStagesStopped {
      val provider = MongoChangeStreamSourceProvider(system, clientSettings, Seq.empty)
      val readOffsetsHandler = () => Future.successful(None)
      val probe = Source
        .futureSource(provider.source(readOffsetsHandler))
        .runWith(TestSink.probe(actorSystem.classicSystem))

      produce(1 to 100).futureValue

      probe.request(1)
      val first = probe.expectNext()
      first.getNamespace.getCollectionName shouldBe "test"

      probe.cancel()
    }
  }
}
