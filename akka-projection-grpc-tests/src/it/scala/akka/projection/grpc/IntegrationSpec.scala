/*
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.grpc.GrpcClientSettings
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.Metadata
import akka.grpc.scaladsl.MetadataBuilder
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.projection.r2dbc.scaladsl.R2dbcHandler
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.projection.scaladsl.Handler
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import org.slf4j.event.Level

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

object IntegrationSpec {

  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.http.server.preview.enable-http2 = on
    akka.persistence.r2dbc {
      query {
        refresh-interval = 500 millis
        # reducing this to have quicker test, triggers backtracking earlier
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

  final case class Processed(projectionId: ProjectionId, envelope: EventEnvelope[String])

  class TestHandler(projectionId: ProjectionId, probe: ActorRef[Processed]) extends Handler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      log.debug2("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }

  class TestR2dbcHandler(projectionId: ProjectionId, probe: ActorRef[Processed])
      extends R2dbcHandler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(session: R2dbcSession, envelope: EventEnvelope[String]): Future[Done] = {
      log.debug2("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }
}

class IntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(IntegrationSpec.config.withFallback(testContainerConf.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {
  import IntegrationSpec._

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext
  private val numberOfTests = 4

  // needs to be unique per test case and known up front for setting up the producer
  case class TestSource(entityType: String, streamId: String, pid: PersistenceId)
  private val testSources = (1 to numberOfTests).map { n =>
    val entityType = nextEntityType()
    val streamId = s"stream_id_$n"
    val pid = nextPid(entityType) // consuming side pid still has entity type
    TestSource(entityType, streamId, pid)
  }
  private val testSourceIterator = testSources.iterator

  class TestFixture {
    val testSource = testSourceIterator.next()
    def streamId = testSource.streamId
    def pid = testSource.pid
    val sliceRange = 0 to 1023
    val projectionId = randomProjectionId()

    val replyProbe = createTestProbe[Done]()
    val processedProbe = createTestProbe[Processed]()

    lazy val entity = spawn(TestEntity(pid))

    private def sourceProvider =
      EventSourcedProvider.eventsBySlices[String](
        system,
        GrpcReadJournal(
          GrpcQuerySettings(streamId).withAdditionalRequestMetadata(
            new MetadataBuilder().addText("x-secret", "top_secret").build()),
          GrpcClientSettings
            .connectToServiceAt("127.0.0.1", grpcPort)
            .withTls(false),
          protobufDescriptors = Nil),
        // FIXME: error prone that it needs to be passed both to GrpcReadJournal and here?
        // but on the consuming side we don't know about the producing side entity types
        streamId,
        sliceRange.min,
        sliceRange.max)

    def spawnAtLeastOnceProjection(): ActorRef[ProjectionBehavior.Command] =
      spawn(
        ProjectionBehavior(
          R2dbcProjection.atLeastOnceAsync(
            projectionId,
            settings = None,
            sourceProvider = sourceProvider,
            handler = () => new TestHandler(projectionId, processedProbe.ref))))

    def spawnExactlyOnceProjection(): ActorRef[ProjectionBehavior.Command] =
      spawn(
        ProjectionBehavior(
          R2dbcProjection.exactlyOnce(
            projectionId,
            settings = None,
            sourceProvider = sourceProvider,
            handler = () => new TestR2dbcHandler(projectionId, processedProbe.ref))))

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

    val eventProducerSources = testSources
      .map(source =>
        EventProducerSource(source.entityType, source.streamId, transformation, EventProducerSettings(system)))
      .toSet

    val authInterceptor = new EventProducerInterceptor {
      def intercept(streamId: String, requestMetadata: Metadata): Future[Done] = {
        if (!requestMetadata.getText("x-secret").contains("top_secret"))
          throw new GrpcServiceException(Status.PERMISSION_DENIED)
        else Future.successful(Done)
      }
    }

    val eventProducerService =
      EventProducer.grpcServiceHandler(eventProducerSources, Some(authInterceptor))

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(eventProducerService)

    val bound =
      Http()
        .newServerAt("127.0.0.1", grpcPort)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.futureValue
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    testContainerConf.stop()
  }

  "A gRPC Projection" must {
    "receive events" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      // start the projection
      val projection = spawnAtLeastOnceProjection()

      val processedA = processedProbe.receiveMessage()
      processedA.envelope.persistenceId shouldBe pid.id
      processedA.envelope.sequenceNr shouldBe 1L
      processedA.envelope.event shouldBe "A"

      val processedB = processedProbe.receiveMessage()
      processedB.envelope.persistenceId shouldBe pid.id
      processedB.envelope.sequenceNr shouldBe 2L
      processedB.envelope.event shouldBe "B"

      entity ! TestEntity.Persist("c")
      val processedC = processedProbe.receiveMessage()
      processedC.envelope.persistenceId shouldBe pid.id
      processedC.envelope.sequenceNr shouldBe 3L
      processedC.envelope.event shouldBe "C"

      projection ! ProjectionBehavior.Stop
      entity ! TestEntity.Stop(replyProbe.ref)
      processedProbe.expectTerminated(projection)
      processedProbe.expectTerminated(entity)
    }

    "filter out events" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b*")
      entity ! TestEntity.Persist("c")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      // start the projection
      val projection = spawnAtLeastOnceProjection()

      val processedA = processedProbe.receiveMessage()
      processedA.envelope.persistenceId shouldBe pid.id
      processedA.envelope.sequenceNr shouldBe 1L
      processedA.envelope.event shouldBe "A"

      // b* is filtered out by the registered transformation

      val processedC = processedProbe.receiveMessage()
      processedC.envelope.persistenceId shouldBe pid.id
      processedC.envelope.sequenceNr shouldBe 3L
      processedC.envelope.event shouldBe "C"

      projection ! ProjectionBehavior.Stop
      entity ! TestEntity.Stop(replyProbe.ref)

      processedProbe.expectTerminated(projection)
      processedProbe.expectTerminated(entity)
    }

    "resume from offset" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      // start the projection
      val projection = spawnExactlyOnceProjection()

      processedProbe.receiveMessage().envelope.event shouldBe "A"
      processedProbe.receiveMessage().envelope.event shouldBe "B"
      processedProbe.expectNoMessage()

      projection ! ProjectionBehavior.Stop
      processedProbe.expectTerminated(projection)
      // start new projection
      val projection2 = spawnExactlyOnceProjection()

      entity ! TestEntity.Persist("c")
      processedProbe.receiveMessage().envelope.event shouldBe "C"

      processedProbe.expectNoMessage()
      projection2 ! ProjectionBehavior.Stop
      entity ! TestEntity.Stop(replyProbe.ref)

      processedProbe.expectTerminated(projection2)
      processedProbe.expectTerminated(entity)
    }

    "deduplicate backtracking events" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Persist("c")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val projection =
        LoggingTestKit
          .custom { event =>
            event.level == Level.TRACE && event.message.matches(
              s"""Received event from \\[127.0.0.1] persistenceId \\[${pid.id
                .replace("|", "\\|")}] with seqNr \\[[123]].*""") && event.message
              .endsWith("source [BT]")
          }
          .withOccurrences(3)
          .expect {
            // start the projection
            spawnExactlyOnceProjection()
          }

      processedProbe.receiveMessage().envelope.event shouldBe "A"
      processedProbe.receiveMessage().envelope.event shouldBe "B"
      processedProbe.receiveMessage().envelope.event shouldBe "C"

      processedProbe.expectNoMessage()
      projection ! ProjectionBehavior.Stop
      entity ! TestEntity.Stop(replyProbe.ref)

      processedProbe.expectTerminated(projection)
      processedProbe.expectTerminated(entity)
    }
  }

}
