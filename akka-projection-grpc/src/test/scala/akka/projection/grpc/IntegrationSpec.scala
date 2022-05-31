/**
 * Copyright (C) 2022 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object IntegrationSpec {

  val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.http.server.preview.enable-http2 = on
    akka.persistence.r2dbc {
      query {
        refresh-interval = 500 millis
      }
    }
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

  object TestEntity {
    sealed trait Command
    final case class Persist(payload: Any) extends Command
    final case class Ping(replyTo: ActorRef[Done]) extends Command
    final case class Stop(replyTo: ActorRef[Done]) extends Command

    def apply(pid: PersistenceId): Behavior[Command] = {
      Behaviors.setup { context =>
        EventSourcedBehavior[Command, Any, String](
          persistenceId = pid,
          "",
          { (_, command) =>
            command match {
              case command: Persist =>
                context.log.debug(
                  "Persist [{}], pid [{}], seqNr [{}]",
                  command.payload,
                  pid.id,
                  EventSourcedBehavior.lastSequenceNumber(context) + 1)
                Effect.persist(command.payload)
              case Ping(replyTo) =>
                replyTo ! Done
                Effect.none
              case Stop(replyTo) =>
                replyTo ! Done
                Effect.stop()
            }
          },
          (_, _) => "")
      }
    }
  }

  final case class Processed(
      projectionId: ProjectionId,
      envelope: EventEnvelope[String])

  class TestHandler(projectionId: ProjectionId, probe: ActorRef[Processed])
      extends Handler[EventEnvelope[String]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[String]): Future[Done] = {
      log.debug("{} Processed {}", projectionId.key, envelope.event)
      probe ! Processed(projectionId, envelope)
      Future.successful(Done)
    }
  }
}

class IntegrationSpec
    extends ScalaTestWithActorTestKit(IntegrationSpec.config)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with LogCapturing {
  import IntegrationSpec._

  override def typedSystem: ActorSystem[_] = system
  private implicit val ec: ExecutionContext = system.executionContext

  class TestFixture {
    val entityType = nextEntityType()
    val pid = nextPid(entityType)
    val sliceRange = 0 to 1023
    val projectionId = randomProjectionId()

    val replyProbe = createTestProbe[Done]()
    val processedProbe = createTestProbe[Processed]()

    lazy val entity = spawn(TestEntity(pid))

    private def sourceProvider =
      EventSourcedProvider.eventsBySlices[String](
        system,
        readJournalPluginId = GrpcReadJournal.Identifier,
        entityType,
        sliceRange.min,
        sliceRange.max)

    lazy val projection = spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          projectionId,
          settings = None,
          sourceProvider = sourceProvider,
          handler = () => new TestHandler(projectionId, processedProbe.ref))))

  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    val transformation =
      Transformation.empty.registerMapper((event: String) => {
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

  "A gRPC Projection" must {
    "receive events" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      // start the projection
      projection

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
    }

    "filter out events" in new TestFixture {
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b*")
      entity ! TestEntity.Persist("c")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      // start the projection
      projection

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
    }
  }

}
