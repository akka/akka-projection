/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import java.time.Instant
import java.time.{ Duration => JDuration }
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.consumer.scaladsl.EventTimestampQuerySpec.config
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike

object EventTimestampQuerySpec {

  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.http.server.preview.enable-http2 = on
    akka.projection.grpc {
      consumer.client {
        host = "127.0.0.1"
        port = $grpcPort
        use-tls = false
      }
      producer {
        query-plugin-id = "akka.persistence.r2dbc.query"
      }
    }
    """)
    .withFallback(ConfigFactory.load("persistence.conf"))
}

class EventTimestampQuerySpec
    extends ScalaTestWithActorTestKit(EventTimestampQuerySpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import EventTimestampQuerySpec.grpcPort

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext
  private val entityType = nextEntityType()
  private val streamId = "stream_id_" + entityType

  class TestFixture {
    val pid = nextPid(entityType)

    val replyProbe = createTestProbe[Done]()

    lazy val entity = spawn(TestEntity(pid))

    lazy val grpcReadJournal = GrpcReadJournal(
      system,
      streamId,
      GrpcClientSettings.fromConfig(config.getConfig("akka.projection.grpc.consumer.client")))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val eventProducerSource =
      EventProducerSource(entityType, streamId, Transformation.identity, EventProducerSettings(system))

    val eventProducerService =
      EventProducer.grpcServiceHandler(eventProducerSource)

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(eventProducerService)

    val bound =
      Http()
        .newServerAt("127.0.0.1", grpcPort)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.futureValue
  }

  "GrpcReadJournal with EventTimestampQuery" must {
    "lookup event timestamp" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val timestampA =
        grpcReadJournal.timestampOf(pid.id, sequenceNr = 1L).futureValue.get
      JDuration
        .between(timestampA, Instant.now())
        .toMillis should (be >= 0L and be <= 3000L)

      val timestampB =
        grpcReadJournal.timestampOf(pid.id, sequenceNr = 2L).futureValue.get
      JDuration
        .between(timestampA, Instant.now())
        .toMillis should (be >= 0L and be <= 3000L)

      if (timestampB != timestampA)
        timestampB.isAfter(timestampA) shouldBe true
    }

    "handle missing event as None" in new TestFixture {
      grpcReadJournal
        .timestampOf(pid.id, sequenceNr = 13L)
        .futureValue
        .isEmpty shouldBe true
    }
  }

}
