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
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object KeepAliveSpec {
  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.loglevel = DEBUG
    akka.http.server.preview.enable-http2 = on
    akka.persistence.r2dbc {
      query {
        refresh-interval = 500 millis
        backtracking.behind-current-time = 3 seconds
      }
    }
    akka.projection.grpc {
      producer {
        query-plugin-id = "akka.persistence.r2dbc.query"
      }
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
  import KeepAliveSpec._

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system

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

  "A gRPC Projection with keep alive" must {
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
  }
}
