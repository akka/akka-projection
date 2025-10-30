/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.producer

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.stream.scaladsl.FlowWithContext
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

object ProducerFilterEndToEndSpec {

  val config: Config = ConfigFactory
    .parseString(s"""
    akka.actor.allow-java-serialization = on
    akka.http.server.preview.enable-http2 = on
    akka.persistence.r2dbc.journal.publish-events = false
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

  private case class Processed(envelope: EventEnvelope[String])

  private val entityType = "ProducerFilterTestEntity"
  object TestEntity {
    case class Command(text: String, replyTo: ActorRef[Done])
    def apply(id: String): Behavior[Command] =
      EventSourcedBehavior[Command, String, Set[String]](
        PersistenceId(entityType, id),
        Set.empty[String], {
          case (_, Command(text, replyTo)) =>
            Effect.persist(text).thenRun(_ => replyTo ! Done)
        },
        (state, event) => state + event)
        .withTaggerForState((state, _) =>
          if (state.exists(_.contains("replicate-it"))) Set("replicate-it") else Set.empty)
  }

}

class ProducerFilterEndToEndSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(ProducerFilterEndToEndSpec.config.withFallback(testContainerConf.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(new TestContainerConf)

  import ProducerFilterEndToEndSpec._

  override def typedSystem: ActorSystem[_] = testKit.system
  private implicit val ec: ExecutionContext = typedSystem.executionContext

  private val grpcPort: Int = SocketUtil.temporaryServerAddress("127.0.0.1").getPort

  private val streamId = "producer-filter-e2e-test"
  private val projectionId = ProjectionId("producer-filter-e2e-projection", "0-1023")
  private def grpcJournalSource() =
    EventSourcedProvider.eventsBySlices[String](
      system,
      GrpcReadJournal(
        GrpcQuerySettings(streamId),
        GrpcClientSettings
          .connectToServiceAt("127.0.0.1", grpcPort)
          .withTls(false),
        protobufDescriptors = Nil),
      streamId,
      // just the one consumer for now
      0,
      1023)

  private val projectionProbe = createTestProbe[Processed]()

  private def spawnProjection(): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceFlow(
          projectionId,
          settings = None,
          sourceProvider = grpcJournalSource(),
          handler = FlowWithContext[EventEnvelope[String], ProjectionContext].map { envelope =>
            projectionProbe.ref ! Processed(envelope)
            Done
          })))

  "A projection with producer a filter" must {

    "Start an event producer service" in {
      val eps = EventProducerSource(entityType, streamId, Transformation.identity, EventProducerSettings(system))
        .withProducerFilter[String](envelope => envelope.tags.contains("replicate-it"))
      val handler = EventProducer.grpcServiceHandler(eps)

      val bound =
        Http()(system)
          .newServerAt("127.0.0.1", grpcPort)
          .bind(ServiceHandler.concatOrNotFound(handler))
          .map(_.addToCoordinatedShutdown(3.seconds))

      bound.futureValue
    }

    "Start a consumer" in {
      spawnProjection()
    }

    "Filter events according to producer filter" in {
      val ackProbe = createTestProbe[Done]()
      val entity1 = testKit.spawn(TestEntity("one"))
      entity1.tell(TestEntity.Command("a", ackProbe.ref))
      ackProbe.receiveMessage()
      entity1.tell(TestEntity.Command("b", ackProbe.ref))
      ackProbe.receiveMessage()

      projectionProbe.expectNoMessage()

      entity1.tell(TestEntity.Command("c-replicate-it", ackProbe.ref))
      ackProbe.receiveMessage()

      val replicatedEvents = projectionProbe.receiveMessages(3, 5.seconds) // behind-current-time + some more time
      replicatedEvents.map(_.envelope.event) shouldBe Seq("a", "b", "c-replicate-it")
      replicatedEvents.head.envelope.tags shouldBe Set.empty
      replicatedEvents.last.envelope.tags shouldBe Set("replicate-it")
    }

  }

}
