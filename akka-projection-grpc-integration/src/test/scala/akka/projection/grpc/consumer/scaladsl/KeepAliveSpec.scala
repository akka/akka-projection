/*
 * Copyright (C) 2022-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.consumer.scaladsl

import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.query.NoOffset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.EnvelopeOrigin
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.internal.proto.EventProducerServiceClient
import akka.projection.grpc.internal.proto.InitReq
import akka.projection.grpc.internal.proto.StreamIn
import akka.projection.grpc.internal.proto.StreamOut
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object KeepAliveSpec {
  val config: Config =
    ConfigFactory.parseString("""
    akka.loglevel = DEBUG
    # low latency queries so that events show up promptly after the idle period
    akka.persistence.r2dbc.query {
      refresh-interval = 500 millis
      backtracking.behind-current-time = 3 seconds
    }
    akka.actor.testkit.typed.filter-leeway = 10s
    """)
}

class KeepAliveSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(KeepAliveSpec.config.withFallback(testContainerConf.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system

  private val grpcPort = testContainerConf.grpcPort
  private val entityType = nextEntityType()
  private val streamId = "stream_id_keep_alive"
  private val pid = nextPid(entityType)

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val eventProducerSource =
      EventProducerSource(entityType, streamId, Transformation.identity, EventProducerSettings(system))

    Http()
      .newServerAt("127.0.0.1", grpcPort)
      .bind(EventProducer.grpcServiceHandler(Set(eventProducerSource)))
      .futureValue
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    testContainerConf.stop()
  }

  "GrpcReadJournal with keep alive" must {
    "keep the event stream alive while idle" in {
      val replyProbe = createTestProbe[Done]()
      val entity = spawn(TestEntity(pid))
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val readJournal = GrpcReadJournal(
        GrpcQuerySettings(streamId).withKeepAliveInterval(1.second).withKeepAliveTimeout(3.seconds),
        GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false),
        protobufDescriptors = Nil)

      val probe = readJournal
        .eventsBySlices[String](streamId, 0, 1023, NoOffset)
        .filterNot(env => EnvelopeOrigin.fromBacktracking(env))
        .runWith(TestSink[EventEnvelope[String]]())
      probe.request(10)

      val envelopeA = probe.expectNext()
      envelopeA.persistenceId shouldBe pid.id
      envelopeA.event shouldBe "a"

      // idle for longer than the keep alive timeout without the stream failing
      probe.expectNoMessage(6.seconds)

      entity ! TestEntity.Persist("b")
      val envelopeB = probe.expectNext()
      envelopeB.persistenceId shouldBe pid.id
      envelopeB.event shouldBe "b"

      probe.cancel()
      readJournal.close().futureValue
    }

    "send keep alives from the producer when requested" in {
      // raw client, since the read journal drops the keep alives
      val client =
        EventProducerServiceClient(GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false))
      val initReq = InitReq(streamId, 0, 1023, offset = Seq.empty, keepAliveIntervalMillis = 1000)
      val probe = client
        .eventsBySlices(Source.single(StreamIn(StreamIn.Message.Init(initReq))).concat(Source.maybe))
        .runWith(TestSink[StreamOut]())
      probe.request(100)

      // first keep alive right away, then at the interval in between the events from the previous test
      probe.expectNext(2.seconds).message.isKeepAlive shouldBe true
      probe.receiveWithin(2500.millis).count(_.message.isKeepAlive) should be >= 1

      probe.cancel()
      client.close().futureValue
    }
  }
}
