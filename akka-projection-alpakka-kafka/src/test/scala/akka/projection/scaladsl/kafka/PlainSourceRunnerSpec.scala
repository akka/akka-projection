/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.projection.testkit.{InMemoryRepository, TransactionalDbRunner}
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

      val repository = new InMemoryRepository[String]
      val runner = new TransactionalDbRunner[Long]("plain-source-kafka")

      val projection =
        KafkaProjections
            .plainSource(consumerDefaults, new TopicPartition(topic1, 0))
            .withEventHandler(TestEventHandler.dbEventHandler(repository))
            .withProjectionRunner(runner)

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

      val repository = new InMemoryRepository[String]
      val runner = new TransactionalDbRunner[Long]("plain-source-kafka")

      val projection1 =
        KafkaProjections
          .plainSource(consumerDefaults, new TopicPartition(topic1, 0))
          // fail handler on "def"
          .withEventHandler(TestEventHandler.dbEventHandler(repository, failPredicate = _ == "def"))
          .withProjectionRunner(runner)

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
        KafkaProjections
          .plainSource(consumerDefaults, new TopicPartition(topic1, 0))
          .withEventHandler(TestEventHandler.dbEventHandler(repository))
          .withProjectionRunner(runner)

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
