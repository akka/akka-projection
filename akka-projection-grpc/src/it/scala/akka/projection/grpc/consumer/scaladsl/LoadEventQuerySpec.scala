/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.consumer.scaladsl

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import io.grpc.Status
import io.grpc.StatusRuntimeException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

class LoadEventQuerySpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(testContainerConf.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext
  private val entityType = nextEntityType()
  private val streamId = "stream_id_" + entityType

  protected override def afterAll(): Unit = {
    super.afterAll()
    testContainerConf.stop()
  }

  class TestFixture {

    val replyProbe = createTestProbe[Done]()
    val pid = nextPid(entityType)

    lazy val entity = spawn(TestEntity(pid))

    lazy val grpcReadJournal = GrpcReadJournal(
      GrpcQuerySettings(streamId),
      GrpcClientSettings
        .connectToServiceAt("127.0.0.1", testContainerConf.grpcPort)
        .withTls(false),
      protobufDescriptors = Nil)
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

    val eventProducerSource = EventProducerSource(entityType, streamId, transformation, EventProducerSettings(system))

    val eventProducerService =
      EventProducer.grpcServiceHandler(eventProducerSource)

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(eventProducerService)

    val bound =
      Http()
        .newServerAt("127.0.0.1", testContainerConf.grpcPort)
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
      env.filtered shouldBe true
      env.eventMetadata shouldBe None
      env.eventOption.isEmpty shouldBe true
    }

    "handle missing event as NOT_FOUND" in new TestFixture {
      val status =
        intercept[StatusRuntimeException] {
          Await.result(grpcReadJournal.loadEnvelope[String](pid.id, sequenceNr = 13L), replyProbe.remainingOrDefault)
          fail("Expected NOT_FOUND")
        }.getStatus
      status.getCode shouldBe Status.NOT_FOUND.getCode
    }
  }

}
