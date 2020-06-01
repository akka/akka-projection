package akka.projection.kafka.internal

import scala.concurrent.Future

import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.ClassicActorSystemProvider
import akka.kafka.ConsumerSettings
import akka.projection.kafka.internal.KafkaSourceProviderImplSpec.TestMetadataClientAdapter
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.GroupOffsets.TopicPartitionKey
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.Source
import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.RunningProjection
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues

object KafkaSourceProviderImplSpec {
  class TestMetadataClientAdapter(topic: String, partitions: Int) extends MetadataClientAdapter {
    override def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      Future.successful((0 until partitions).map(i => new TopicPartition("topic", i) -> 0L).toMap)
    override def numPartitions(topics: Set[String]): Future[Int] = Future.successful(partitions)
    override def stop(): Unit = ()
  }

  // TODO: Adapt TestProjection from ProjectionTestKitSpec instead of implementing something completely custom
  class TestProjection(
      sourceProvider: SourceProvider[GroupOffsets, ConsumerRecord[String, String]],
      topic: String,
      partitions: Int)
      extends Projection[ConsumerRecord[String, String]] {

    val groupOffsets = GroupOffsets(
      (0 until partitions).map(i => TopicPartitionKey(new TopicPartition(topic, i)) -> 0L).toMap)

    override def projectionId: ProjectionId = ProjectionId("name", "key")
    override def withSettings(settings: ProjectionSettings): Projection[ConsumerRecord[String, String]] = this

    override private[projection] def mappedSource()(implicit system: ActorSystem[_]) = {
      val futSource = sourceProvider.source(() => Future.successful(Option(groupOffsets)))

      Source
        .futureSource(futSource)
        // TODO: how do verify calls to sourceProvider methods?
        .mapConcat { env =>
          val offset = sourceProvider.extractOffset(env)
          sourceProvider.verifyOffset(offset) match {
            case VerificationSuccess         => List(offset -> env)
            case VerificationFailure(reason) => Nil
          }
        }
        .map(_ => Done)
    }

    override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection = ???
  }
}

class KafkaSourceProviderImplSpec
    extends ScalaTestWithActorTestKit(ConfigFactory.load())
    with LogCapturing
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with OptionValues
    with PatienceConfiguration {

  "The KafkaSourceProviderImpl" must {
    "successfully verify offsets from assigned partitions" ignore {
      val topic = "topic"
      val partitions = 2

      val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
      val metadataClient = new TestMetadataClientAdapter(topic, partitions)

      val provider = new KafkaSourceProviderImpl(system, settings, Set(topic), metadataClient)

    }
  }
}
