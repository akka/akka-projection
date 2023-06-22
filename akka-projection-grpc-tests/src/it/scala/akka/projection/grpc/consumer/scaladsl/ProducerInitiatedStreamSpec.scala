/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.consumer.scaladsl

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.grpc.GrpcClientSettings
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
import akka.projection.grpc.internal.ReverseGrpcReadJournal
import akka.projection.grpc.internal.ReverseEventProducer
import akka.projection.grpc.internal.proto.EventConsumerServiceHandler
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.stream.scaladsl.FlowWithContext
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

object ProducerInitiatedStreamSpec {
  def config =
    ConfigFactory.parseString("""
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

       # on each producer
       akka.projection.grpc.producer {
         # FIXME does this need to be a uuid in practice? device unique? document!
         producer-id = "max_martin"
         reverse-destinations = [{
           # akka grpc client block
           client {
             host = "localhost"
             port = 8080
             use-tls = false
           }
         }]
       }
      """)

  private val entityType = "ProducerInitiatedStreamTestEntity"
  private case class Processed(envelope: EventEnvelope[String])
  object TestEntity {
    case class Command(text: String, replyTo: ActorRef[Done])

    def apply(id: String): Behavior[Command] =
      EventSourcedBehavior[Command, String, Set[String]](PersistenceId(entityType, id), Set.empty[String], {
        case (_, Command(text, replyTo)) =>
          Effect.persist(text).thenRun(_ => replyTo ! Done)
      }, (state, event) => state + event)
  }

}

class ProducerInitiatedStreamSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(ProducerInitiatedStreamSpec.config.withFallback(testContainerConf.config))
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {
  import ProducerInitiatedStreamSpec._

  def this() = this(new TestContainerConf)

  override def typedSystem: ActorSystem[_] = system

  private val streamId = "producer-initiated-stream-e2e-test"
  private val projectionId = ProjectionId("producer-initiated-stream-e2e-projection", "0-1023")

  private def grpcJournalSource() =
    EventSourcedProvider.eventsBySlices[String](
      system,
      GrpcReadJournal(
        GrpcQuerySettings(streamId),
        // FIXME settings that doesn't actively connect?
        //       or do we do a special SourceProvider?
        GrpcClientSettings
          .connectToServiceAt("127.0.0.1", 9090)
          .withTls(false),
        protobufDescriptors = Nil),
      streamId,
      // just the one consumer for now
      // FIXME will we support slicing at all or always consume all?
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

  "A gRPC producer initiating streams" should {

    "project events" in {
      // on the "consumer side"
      // FIXME bind consumer service from inside projections gRPC?
      // FIXME limit what stream ids to allow?
      // FIXME consumer filters?
      val handler = EventConsumerServiceHandler(new ReverseGrpcReadJournal(system))
      val _ = Http(system).newServerAt("localhost", 8080).bind(handler)
      spawnProjection()

      // on the "producer side"
      // FIXME startup producer from inside projections gRPC, no EventProducerService
      // FIXME additional request metadata for auth
      // FIXME producer filters?
      val eps = EventProducerSource(entityType, streamId, Transformation.identity, EventProducerSettings(system))
      spawn(ReverseEventProducer(EventProducer.eventProducer(Set(eps), None)), "ReverseProducer")

      // FIXME write some events on the "producer side"
      val probe = createTestProbe[Any]()
      val entity = spawn(TestEntity("id1"))
      entity ! TestEntity.Command("one", probe.ref)
      probe.receiveMessage()

      // consumer should see that vent
      projectionProbe.receiveMessage()
    }

  }

}
