/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.projection.testkit.{ TestEventHandler, TestInMemoryRepository, TestTransactionalDbRunner }
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.kafka.common.TopicPartition
import org.scalatest.OptionValues
import org.scalatest.concurrent.PatienceConfiguration.Timeout

import scala.concurrent.duration._

class PlainSourceRunnerSpec extends TestcontainersKafkaSpec with OptionValues {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)

  "Kafka Projection using at-most-once delivery" must {

    "deliver events to ProjectionHandler" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]
      val runner = new TestTransactionalDbRunner[Long]("plain-source-kafka")

      val projection =
        KafkaProjection(
          KafkaSourceProviders.plainSource(consumerDefaults, new TopicPartition(topic1, 0)),
          TestEventHandler(repository),
          runner)

      runProjection(projection) {
        eventually {
          withClue("all messages are delivered to projection") {
            repository.size shouldBe Messages.size
          }

          withClue("projected stream contains all elements") {
            repository.list shouldBe Messages
            runner.lastOffset.value shouldBe 2
          }
        }
      }
    }

    "re-delivered an event after a failure (at-least-once)" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      produceString(topic1, Messages).futureValue(Timeout(remainingOrDefault))

      val repository = new TestInMemoryRepository[String]
      val runner = new TestTransactionalDbRunner[Long]("plain-source-kafka")

      val projection1 =
        KafkaProjection(
          KafkaSourceProviders.plainSource(consumerDefaults, new TopicPartition(topic1, 0)),
          // fail handler on "def"
          TestEventHandler(repository, failPredicate = (s: String) => s == "def"),
          runner)

      runProjection(projection1) {
        eventually {
          withClue("one message is delivery before failing") {
            repository.size shouldBe 1
          }

          withClue("projected list contains only one element, 'def' failed") {
            repository.list shouldBe List("abc")
            runner.lastOffset.value shouldBe 0
          }
        }
      }

      //re-try without failing
      val projection2 =
        KafkaProjection(
          KafkaSourceProviders.plainSource(consumerDefaults, new TopicPartition(topic1, 0)),
          TestEventHandler(repository),
          runner)

      runProjection(projection2) {
        eventually {
          withClue("all messages are delivered to projection") {
            repository.size shouldBe Messages.size
          }

          withClue("projected list contains all elements, 'def' is re-delivered and offset is respected") {
            repository.list shouldBe Messages
            runner.lastOffset.value shouldBe 2
          }
        }
      }
    }
  }
}
