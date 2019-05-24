/*
 * Copyright (C) 2019 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.scaladsl.kafka

import akka.Done
import akka.kafka.Subscriptions
import akka.kafka.scaladsl.Consumer
import akka.projection.testkit.{DBIO, DbProjectionHandler, InMemoryRepository}
import akka.projection.scaladsl.Projection
import akka.stream.scaladsl.{Keep, Sink}
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import akka.stream.testkit.scaladsl.TestSink

import scala.concurrent.Await
import scala.concurrent.duration._

class AtLeastOnceRunnerSpec extends TestcontainersKafkaSpec {

  implicit val patience: PatienceConfig = PatienceConfig(30.seconds, 500.millis)


  "Kafka Projection using at-least-once delivery" must {

    "deliver events to ProjectionHandler" in assertAllStagesStopped {

      val Messages = "abc" :: "def" :: "ghi" :: Nil
      val topic1 = createTopic(1)
      val group1 = createGroupId(1)
      Await.result(produceString(topic1, Messages), remainingOrDefault)

      val repository = new InMemoryRepository[String]
      val handler = new DbProjectionHandler[String] {
        override def handleEvent(event: String): DBIO[Done] = {
          repository.save(event)
        }
      }

      // this is how the API looks like when using it directly. 
      //      Projection(
      //        KafkaSourceProviders.committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1)),
      //        KafkaExtractors.committableSource[String, String],
      //        KafkaProjectionRunners.atLeastOnceRunner,
      //        handler.asAsyncHandler
      //      ).start()

      val projection =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .withAtLeastOnce
          .withEventHandler(handler.asAsyncHandler)

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
      Await.result(produceString(topic1, Messages), remainingOrDefault)

      val repository = new InMemoryRepository[String]

      def handler(failPredicate: String => Boolean = _ => false) = new DbProjectionHandler[String] {
        override def handleEvent(event: String): DBIO[Done] = {
          if (failPredicate(event))
            throw new RuntimeException(s"Failed on event $event")
          else
            repository.save(event)
        }
      }

      val projection1 =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .withAtLeastOnce
          // fail handler on "def"
          .withEventHandler(handler(failPredicate = _ == "def").asAsyncHandler)

      runProjection(projection1) {
        eventually {
          withClue("one message is delivery before failing") {
            repository.size shouldBe 1
          }

          withClue("projected stream contains only one element ") {
            repository.list shouldBe List("abc")
          }
        }
      }

      //re-try without failing
      val projection2 =
        KafkaProjections
          .committableSource(consumerDefaults.withGroupId(group1), Subscriptions.topics(topic1))
          .withAtLeastOnce
          .withEventHandler(handler().asAsyncHandler)

      runProjection(projection2) {
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
  }

}
