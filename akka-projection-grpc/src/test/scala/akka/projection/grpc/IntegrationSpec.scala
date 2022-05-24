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
import akka.grpc.scaladsl.ServerReflection
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
import akka.projection.grpc.proto.EventReplicationService
import akka.projection.grpc.proto.EventReplicationServiceHandler
import akka.projection.grpc.query.scaladsl.GrpcReadJournal
import akka.projection.grpc.service.EventReplicationServiceImpl
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.projection.scaladsl.Handler
import akka.testkit.SocketUtil
import com.google.protobuf.any.{ Any => ProtoAny }
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
      query {
        port = $grpcPort
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
      envelope: EventEnvelope[
        ProtoAny]) // FIXME change this to EventEnvelope[String] when serialization is in place

  class TestHandler(projectionId: ProjectionId, probe: ActorRef[Processed])
      extends Handler[EventEnvelope[ProtoAny]] {
    private val log = LoggerFactory.getLogger(getClass)

    override def process(envelope: EventEnvelope[ProtoAny]): Future[Done] = {
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

  {
    val eventReplicationService = new EventReplicationServiceImpl(system)

    val service: HttpRequest => Future[HttpResponse] =
      ServiceHandler.concatOrNotFound(
        EventReplicationServiceHandler.partial(eventReplicationService),
        // ServerReflection enabled to support grpcurl without import-path and proto parameters
        ServerReflection.partial(List(EventReplicationService)))

    val bound =
      Http()
        .newServerAt("127.0.0.1", grpcPort)
        .bind(service)
        .map(_.addToCoordinatedShutdown(3.seconds))

    bound.futureValue
  }

  "A gRPC Projection" must {
    "receive events" in {

      val entityType = nextEntityType()
      val pid = nextPid(entityType)
      val sliceRange = 0 to 1023
      val projectionId = randomProjectionId()

      val replyProbe = createTestProbe[Done]()
      val processedProbe = createTestProbe[Processed]()

      val entity = spawn(TestEntity(pid))
      entity ! TestEntity.Persist("a")
      entity ! TestEntity.Persist("b")
      entity ! TestEntity.Ping(replyProbe.ref)
      replyProbe.receiveMessage()

      val sourceProvider =
        EventSourcedProvider.eventsBySlices[ProtoAny](
          system,
          readJournalPluginId = GrpcReadJournal.Identifier,
          entityType,
          sliceRange.min,
          sliceRange.max)

      val projection = spawn(
        ProjectionBehavior(
          R2dbcProjection.atLeastOnceAsync(
            projectionId,
            settings = None,
            sourceProvider = sourceProvider,
            handler = () => new TestHandler(projectionId, processedProbe.ref))))

      val processedA = processedProbe.receiveMessage()
      processedA.envelope.persistenceId shouldBe pid.id
      processedA.envelope.sequenceNr shouldBe 1L
      // FIXME processedA.envelope.event shouldBe "a"

      val processedB = processedProbe.receiveMessage()
      processedB.envelope.persistenceId shouldBe pid.id
      processedB.envelope.sequenceNr shouldBe 2L
      // FIXME processedA.envelope.event shouldBe "b"

      entity ! TestEntity.Persist("c")
      val processedC = processedProbe.receiveMessage()
      processedC.envelope.persistenceId shouldBe pid.id
      processedC.envelope.sequenceNr shouldBe 3L
      // FIXME processedA.envelope.event shouldBe "c"

      projection ! ProjectionBehavior.Stop
      entity ! TestEntity.Stop(replyProbe.ref)
    }
  }

}
