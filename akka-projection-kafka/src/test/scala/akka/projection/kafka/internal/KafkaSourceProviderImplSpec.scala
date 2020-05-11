/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.kafka.internal

import scala.concurrent.Await
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
import akka.projection.RunningProjection
import akka.projection.StatusObserver
import akka.projection.internal.ActorHandlerInit
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.ProjectionSettings
import akka.projection.internal.RestartBackoffSettings
import akka.projection.internal.SettingsImpl
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
import org.scalatest.wordspec.AnyWordSpecLike

class KafkaSourceProviderImplSpec extends ScalaTestWithActorTestKit with LogCapturing with AnyWordSpecLike {

  val projectionTestKit: ProjectionTestKit = ProjectionTestKit(testKit)
  implicit val ec = system.classicSystem.dispatcher

  "The KafkaSourceProviderImpl" must {
    "successfully verify offsets from assigned partitions" in {
      val topic = "topic"
      val partitions = 2
      val settings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
        .withBootstrapServers("localhost:9092")
      val metadataClient = new TestMetadataClientAdapter(partitions)
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
        provider.partitionHandler.onRevoke(Set.empty, null)

        sinkProbe.request(10)
        var records = projection.processNextN(10)
        sinkProbe.expectNextN(10)

        withClue("checking: processed records contain 5 from each partition") {
          records.length shouldBe 10
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 5
        }

        // assign only tp0 to this projection
        provider.partitionHandler.onAssign(Set(tp0), null)
        provider.partitionHandler.onRevoke(Set(tp1), null)

        // only 5 records should remain, because the other 5 were filtered out
        sinkProbe.request(5)
        records = projection.processNextN(5)
        sinkProbe.expectNextN(5)

        withClue("checking: after rebalance processed records should only have records from partition 0") {
          records.count(_.partition() == tp0.partition()) shouldBe 5
          records.count(_.partition() == tp1.partition()) shouldBe 0
        }
      }

      projection.cancelProbe()
    }
  }

  class TestMetadataClientAdapter(partitions: Int) extends MetadataClientAdapter {
    override def getBeginningOffsets(assignedTps: Set[TopicPartition]): Future[Map[TopicPartition, Long]] =
      Future.successful((0 until partitions).map(i => new TopicPartition("topic", i) -> 0L).toMap)
    override def numPartitions(topics: Set[String]): Future[Int] = Future.successful(partitions)
    override def stop(): Unit = ()
  }

  // FIXME: Copied mostly from ProjectionTestKitSpec.
  // Maybe a `TestProjection` could be abstracted out and reused to reduce test boilerplate
  private[akka] case class TestProjection(
      sourceProvider: SourceProvider[GroupOffsets, ConsumerRecord[String, String]],
      topic: String,
      partitions: Int)
      extends Projection[ConsumerRecord[Int, Int]]
      with SettingsImpl[TestProjection] {

    val groupOffsets: GroupOffsets = GroupOffsets(
      (0 until partitions).map(i => TopicPartitionKey(new TopicPartition(topic, i)) -> 0L).toMap)

    val (processedQueue, processedProbe) = Source
      .queue[ConsumerRecord[String, String]](0, OverflowStrategy.backpressure)
      .toMat(TestSink.probe(system.classicSystem))(Keep.both)
      .run()

    def processNextN(n: Long): Seq[ConsumerRecord[String, String]] = {
      processedProbe.request(n)
      processedProbe.expectNextN(n)
    }

    def cancelProbe(): Unit = processedProbe.cancel()

    private lazy val internalState = new InternalProjectionState()

    override def projectionId: ProjectionId = ProjectionId("name", "key")
    override def withSettings(settings: ProjectionSettings): TestProjection = this
    override def withRestartBackoffSettings(restartBackoff: RestartBackoffSettings): TestProjection = this
    override def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): TestProjection = this
    override def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): TestProjection = this

    override private[projection] def mappedSource()(implicit system: ActorSystem[_]) =
      internalState.mappedSource()

    private[akka] def actorHandlerInit[T]: Option[ActorHandlerInit[T]] = None

    override private[projection] def run()(implicit system: ActorSystem[_]): RunningProjection =
      internalState.newRunningInstance()

    override val statusObserver: StatusObserver[ConsumerRecord[Int, Int]] = NoopStatusObserver

    override def withStatusObserver(
        observer: StatusObserver[ConsumerRecord[Int, Int]]): Projection[ConsumerRecord[Int, Int]] =
      this // no need for StatusObserver in tests

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
            case (offset, _) =>
              sourceProvider.verifyOffset(offset) match {
                case VerificationSuccess    => true
                case VerificationFailure(_) => false
              }
          }
          .map {
            case (_, record) =>
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

      override def stop(): Future[Done] = {
        killSwitch.shutdown()
        futureDone
      }
    }
  }
}
