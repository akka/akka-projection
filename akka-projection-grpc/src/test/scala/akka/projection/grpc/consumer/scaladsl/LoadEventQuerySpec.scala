/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.NotUsed
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.PersistenceQuery
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.scalatest.wordspec.AnyWordSpecLike

object LoadEventQuerySpec {

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

class LoadEventQuerySpec
    extends ScalaTestWithActorTestKit(LoadEventQuerySpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import LoadEventQuerySpec.grpcPort

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  class TestFixture {
    val entityType = nextEntityType()
    val pid = nextPid(entityType)

    val replyProbe = createTestProbe[Done]()

    lazy val entity = spawn(TestEntity(pid))

    lazy val grpcReadJournal = PersistenceQuery(system)
      .readJournalFor[GrpcReadJournal](GrpcReadJournal.Identifier)
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val transformation =
      Transformation.empty.registerAsyncMapper((event: String) => {
        if (event.contains("*"))
          Future.successful(None)
        else
          Future.successful(Some(event.toUpperCase))
      })

    val eventProducerService = EventProducer.grpcServiceHandler(transformation)

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(eventProducerService)

    val bound =
      Http()
        .newServerAt("127.0.0.1", grpcPort)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.futureValue
  }

  "GrpcReadJournal with LoadEventQuery" must {
    "load event" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      grpcReadJournal
        .loadEnvelope[String](pid.id, sequenceNr = 1L)
        .futureValue
        .event shouldBe "A"

      grpcReadJournal
        .loadEnvelope[String](pid.id, sequenceNr = 2L)
        .futureValue
        .event shouldBe "B"
    }

    "load filtered event" in new TestFixture {
      entity ! TestEntity.Persist("a*")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val env = grpcReadJournal
        .loadEnvelope[String](pid.id, sequenceNr = 1L)
        .futureValue
      env.eventOption.isEmpty shouldBe true
      env.eventMetadata shouldBe Some(NotUsed)
    }

    "handle missing event as NOT_FOUND" in new TestFixture {
      val status =
        intercept[StatusRuntimeException] {
          Await.result(
            grpcReadJournal.loadEnvelope[String](pid.id, sequenceNr = 13L),
            replyProbe.remainingOrDefault)
          fail("Expected NOT_FOUND")
        }.getStatus
      status.getCode shouldBe Status.NOT_FOUND.getCode
    }
  }

}
