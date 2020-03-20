/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.projection.testkit.{TestEventHandler, TestInMemoryRepository}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration._

class CommittableSourceAtLeastOnceSpec extends TestcontainersKafkaSpec {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  "Kafka Projection using at-least-once delivery" must {

    "deliver events to ProjectionHandler" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]
      val handler = TestEventHandler(repository)

      val src = KafkaConsumer.atLeastOnce(
        consumerDefaults.withGroupId(group1),
        Subscriptions.topics(topic1),
        committerDefaults.withMaxBatch(1)) { msg =>
        handler.onEvent(msg.record.value())
      }

      val projection = KafkaProjection(src)

      runProjection(projection) {
        eventually {
          withClue("all messages are delivered to projection") {
            repository.size shouldBe Messages.size
          }

          withClue("projected stream contains all elements") {
            repository.list shouldBe Messages
          }
        }
      }

    }

    "re-delivered an event after a failure (at-least-once)" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]

      // fail handler on "def"
      val failingHandler = TestEventHandler(repository, failPredicate = (s: String) => s == "def")

      val failingSrc = KafkaConsumer.atLeastOnce(
        consumerDefaults.withGroupId(group1),
        Subscriptions.topics(topic1),
        committerDefaults.withMaxBatch(1)) { msg =>
        failingHandler.onEvent(msg.record.value())
      }

      val projection1 = KafkaProjection(failingSrc)

      runProjection(projection1) {
        eventually {
          withClue("one message is delivery before failing") {
            repository.size shouldBe 1
          }

          withClue("projected list contains only one element, 'def' failed") {
            repository.list shouldBe List("abc")
          }
        }
      }

      //re-try without failing
      val handler = TestEventHandler(repository)

      val src = KafkaConsumer.atLeastOnce(
        consumerDefaults.withGroupId(group1),
        Subscriptions.topics(topic1),
        committerDefaults.withMaxBatch(1)) { msg =>
        handler.onEvent(msg.record.value())
      }

      val projection2 = KafkaProjection(src)

      runProjection(projection2) {
        eventually {
          withClue("all messages are delivered to projection") {
            repository.size shouldBe Messages.size
          }

          withClue("projected list contains all elements, 'def' is re-delivered") {
            repository.list shouldBe Messages
          }
        }
      }
    }
  }

}
