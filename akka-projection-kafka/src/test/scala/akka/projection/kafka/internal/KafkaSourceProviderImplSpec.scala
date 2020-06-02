package akka.projection.kafka.internal

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.kafka.ConsumerSettings
import akka.kafka.scaladsl.Consumer
import akka.projection.OffsetVerification.VerificationFailure
import akka.projection.OffsetVerification.VerificationSuccess
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.RunningProjection
import akka.projection.kafka.GroupOffsets
import akka.projection.kafka.GroupOffsets.TopicPartitionKey
import akka.projection.kafka.internal.KafkaSourceProviderImpl.ReadOffsets
import akka.projection.scaladsl.SourceProvider
import akka.projection.testkit.scaladsl.ProjectionTestKit
import akka.stream.KillSwitches
import akka.stream.OverflowStrategy
import akka.stream.SharedKillSwitch
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.BeforeAndAfterAll
import org.scalatest.OptionValues
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

class KafkaSourceProviderImplSpec
    extends ScalaTestWithActorTestKit
    with LogCapturing
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with OptionValues
    with PatienceConfiguration {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)
  implicit val ec = system.classicSystem.dispatcher

  "The KafkaSourceProviderImpl" must {
    "successfully verify offsets from assigned partitions" in {
      val topic = "topic"
      val partitions = 2

      val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
      val metadataClient = new TestMetadataClientAdapter(topic, partitions)
      val tp0 = new TopicPartition(topic, 0)
      val tp1 = new TopicPartition(topic, 1)

      val consumerRecords =
        for (n <- 0 to 10; tp <- List(tp0, tp1))
          yield new ConsumerRecord(tp.topic(), tp.partition(), n, n.toString, n.toString)

      val consumerSource = Source(consumerRecords)
        .mapMaterializedValue(_ => Consumer.NoopControl)

      val provider = new KafkaSourceProviderImpl(system, settings, Set(topic), metadataClient) {
        override protected[internal] def _source(
            readOffsets: ReadOffsets,
            numPartitions: Int): Source[ConsumerRecord[String, String], Consumer.Control] =
          consumerSource
      }

      val projection = TestProjection(provider, topic, partitions)

      projectionTestKit.runWithTestSink(projection) { sinkProbe =>
        provider.partitionHandler.onAssign(Set(tp0, tp1), null)

        sinkProbe.request(10)

        //projection.verifyNextN(10)
        var records = projection.processNextN(10)
        sinkProbe.expectNextN(10)

        withClue("checking: processed records contain 5 from each partition") {
          records.foreach(println)
          records.length shouldBe 10
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 5
        }

        println("reassigning partitions")
        provider.partitionHandler.onAssign(Set(tp0), null)

        sinkProbe.request(5)
        //projection.verifyNextN(10)
        records = projection.processNextN(5)
        sinkProbe.expectNextN(5)

        withClue("checking: after rebalance processed records should only have records from partition 0") {
          records.foreach(println)
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 0
        }
      }

      projection.cancelProbes()
    }
  }

  class TestMetadataClientAdapter(topic: String, partitions: Int) extends MetadataClientAdapter {
    override def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      Future.successful((0 until partitions).map(i => new TopicPartition("topic", i) -> 0L).toMap)
    override def numPartitions(topics: Set[String]): Future[Int] = Future.successful(partitions)
    override def stop(): Unit = ()
  }

  // TODO: Adapt TestProjection from ProjectionTestKitSpec instead of implementing something completely custom
  case class TestProjection(
      sourceProvider: SourceProvider[GroupOffsets, ConsumerRecord[String, String]],
      topic: String,
      partitions: Int)
      extends Projection[ConsumerRecord[Int, Int]] {

    val groupOffsets = GroupOffsets(
      (0 until partitions).map(i => TopicPartitionKey(new TopicPartition(topic, i)) -> 0L).toMap)
    val (processedQueue, processedProbe) = Source
      .queue[ConsumerRecord[String, String]](0, OverflowStrategy.backpressure)
      .toMat(TestSink.probe(system.classicSystem))(Keep.both)
      .run()
    val (verifiedQueue, verifiedProbe) = Source
      .queue[Done](0, OverflowStrategy.backpressure)
      .toMat(TestSink.probe(system.classicSystem))(Keep.both)
      .run()

    def verifyNextN(n: Long): Unit = {
      verifiedProbe.request(n)
      processedProbe.request(n)
      verifiedProbe.expectNextN(n)
      //verifiedProbe.expectNextN(n)
    }

    def processNextN(n: Long): Seq[ConsumerRecord[String, String]] = {
      //verifiedProbe.request(n)
      processedProbe.request(n)
      //verifiedProbe.expectNextN(n)
      processedProbe.expectNextN(n)
    }

    def cancelProbes(): Unit = {
      verifiedProbe.cancel()
      processedProbe.cancel()
    }

    private lazy val internalState = new InternalProjectionState()

    override def projectionId: ProjectionId = ProjectionId("name", "key")
    override def withSettings(settings: ProjectionSettings): Projection[ConsumerRecord[Int, Int]] = this

    override private[projection] def mappedSource()(implicit system: ActorSystem[_]) =
      internalState.mappedSource()

    override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
      internalState.newRunningInstance()

    /*
     * INTERNAL API
     * This internal class will hold the KillSwitch that is needed
     * when building the mappedSource and when running the projection (to stop)
     */
    private class InternalProjectionState()(implicit val system: ActorSystem[_]) {

      private val killSwitch = KillSwitches.shared(projectionId.id)

      def mappedSource(): Source[Done, _] = {
        val futSource = sourceProvider.source(() => Future.successful(Option(groupOffsets)))
        Source
          .futureSource(futSource)
          .map(env => (sourceProvider.extractOffset(env), env))
          .filter {
            case (offset, record) =>
              //Await.result(verifiedQueue.offer(Done), 10.millis)
              println(s"verifying offset $offset")
              val pred = sourceProvider.verifyOffset(offset) match {
                case VerificationSuccess    => true
                case VerificationFailure(_) => false
              }
              println(s"verified offset $offset, predicate: $pred")
              pred
          }
          .map {
            case (_, record) =>
              println(s"processing: $record")
              Await.result(processedQueue.offer(record), 10.millis)
              Done
          }
      }

      def newRunningInstance(): RunningProjection =
        new TestRunningProjection(mappedSource(), killSwitch)
    }

    private class TestRunningProjection(val source: Source[Done, _], killSwitch: SharedKillSwitch)
        extends RunningProjection {

      private val futureDone = source.run()

      override def stop()(implicit ec: ExecutionContext): Future[Done] = {
        killSwitch.shutdown()
        futureDone
      }
    }
  }
}
