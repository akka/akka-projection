/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.kafka.Subscriptions
import akka.projection.testkit.{TestEventHandler, TestInMemoryRepository}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration._

class CommittableSourceAtMostOnceRunnerSpec extends TestcontainersKafkaSpec {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)


  "Kafka Projection using at-most-once delivery" must {

    "deliver events to ProjectionHandler" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]

      val projection =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .withEventHandler(TestEventHandler(repository).asAsyncHandler)
          .withAtMostOnce

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

    "not re-delivered an event after a failure (at-most-once)" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)

      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]

      val projection1 =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          // fail handler on "def"
          .withEventHandler(TestEventHandler(repository, failPredicate = (s: String) => s == "def").asAsyncHandler)
          .withAtMostOnce

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
      val projection2 =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .withEventHandler(TestEventHandler(repository).asAsyncHandler)
          .withAtMostOnce

      runProjection(projection2) {
        eventually {
          withClue("not all messages are delivered to projection") {
            repository.size shouldBe 2
          }

          withClue("projected list is missing 'def'") {
            repository.list shouldBe List("abc", "ghi")
          }
        }
      }
    }
  }

}
