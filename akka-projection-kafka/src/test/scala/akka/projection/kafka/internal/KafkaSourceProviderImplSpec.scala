/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import scala.concurrent.duration._
import scala.concurrent.Future

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.testkit.typed.scaladsl.TestProbe
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.projection.MergeableOffset
import akka.projection.ProjectionId
import akka.projection.scaladsl.Handler
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.projection.testkit.scaladsl.TestProjection
import akka.stream.scaladsl.Source
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.wordspec.AnyWordSpecLike

object KafkaSourceProviderImplSpec {
  private val TestProjectionId = ProjectionId("test-projection", "00")

  def handler(probe: TestProbe[ConsumerRecord[String, String]]): Handler[ConsumerRecord[String, String]] =
    new Handler[ConsumerRecord[String, String]] {
      override def process(env: ConsumerRecord[String, String]): Future[Done] = {
        probe.ref ! env
        Future.successful(Done)
      }
    }

  private class TestMetadataClientAdapter(partitions: Int) extends MetadataClientAdapter {
    override def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      Future.successful((0 until partitions).map(i => new TopicPartition("topic", i) -> 0L).toMap)
    override def numPartitions(topics: Set[String]): Future[Int] = Future.successful(partitions)
    override def stop(): Unit = ()
  }
}

class KafkaSourceProviderImplSpec extends ScalaTestWithActorTestKit with LogCapturing with AnyWordSpecLike {
  import KafkaSourceProviderImplSpec._

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)
  implicit val ec = system.classicSystem.dispatcher

  "The KafkaSourceProviderImpl" must {

    "successfully verify offsets from assigned partitions" in {
      val topic = "topic"
      val partitions = 2
      val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
        .withGroupId("group-id")
      val metadataClient = new TestMetadataClientAdapter(partitions)
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)

      val consumerRecords =
        for (n <- 0 to 10; tp <- List(tp0, tp1))
          yield new ConsumerRecord(tp.topic(), tp.partition(), n, n.toString, n.toString)

      val consumerSource = Source(consumerRecords)
        .mapMaterializedValue(_ => Consumer.NoopControl)

      val provider =
        new KafkaSourceProviderImpl(
          system,
          settings,
          Set(topic),
          () => metadataClient,
          KafkaSourceProviderSettings(system)) {
          override protected[internal] def _source(
              readOffsets: () => Future[Option[MergeableOffset[java.lang.Long]]],
              numPartitions: Int,
              metadataClient: MetadataClientAdapter): Source[ConsumerRecord[String, String], Consumer.Control] =
            consumerSource
        }

      val probe = testKit.createTestProbe[ConsumerRecord[String, String]]()
      val projection = TestProjection(TestProjectionId, provider, () => handler(probe))

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        provider.partitionHandler.onAssign(Set(tp0, tp1), null)
        provider.partitionHandler.onRevoke(Set.empty, null)

        sinkProbe.request(10)
        sinkProbe.expectNextN(10)
        var records = probe.receiveMessages(10)

        withClue("checking: processed records contain 5 from each partition") {
          records.length shouldBe 10
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 5
        }

        // assign only tp0 to this projection
        provider.partitionHandler.onAssign(Set(tp0), null)
        provider.partitionHandler.onRevoke(Set(tp1), null)

        // drain any remaining messages that were processed before rebalance because of async stages in the internal
        // projection stream
        eventually(probe.expectNoMessage(1.millis))

        // only records from partition 0 should remain, because the rest were filtered
        sinkProbe.request(5)
        sinkProbe.expectNextN(5)
        records = probe.receiveMessages(5)

        withClue("checking: after rebalance processed records should only have records from partition 0") {
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 0
        }
      }
    }
  }
}
