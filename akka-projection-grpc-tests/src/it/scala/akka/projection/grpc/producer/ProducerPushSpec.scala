/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.producer

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.internal.EventConsumerServiceImpl
import akka.projection.grpc.internal.EventPusher
import akka.projection.grpc.internal.FilteredPayloadMapper
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.internal.proto.EventConsumerServiceHandler
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

object ProducerPushSpec {

  private val tripleQuote = "\"\"\""

  def config = ConfigFactory.parseString(s"""
     akka.http.server.enable-http2 = on

     # producer uses default journal

     # Projection schema if using h2
     akka.persistence.r2dbc.h2.additional-init = $tripleQuote
       CREATE TABLE IF NOT EXISTS akka_projection_offset_store (
        projection_name VARCHAR(255) NOT NULL,
        projection_key VARCHAR(255) NOT NULL,
        current_offset VARCHAR(255) NOT NULL,
        manifest VARCHAR(32) NOT NULL,
        mergeable BOOLEAN NOT NULL,
        last_updated BIGINT NOT NULL,
        PRIMARY KEY(projection_name, projection_key)
      );
      CREATE TABLE IF NOT EXISTS akka_projection_timestamp_offset_store (
        projection_name VARCHAR(255) NOT NULL,
        projection_key VARCHAR(255) NOT NULL,
        slice INT NOT NULL,
        persistence_id VARCHAR(255) NOT NULL,
        seq_nr BIGINT NOT NULL,
        timestamp_offset timestamp with time zone NOT NULL,
        timestamp_consumed timestamp with time zone NOT NULL,
        PRIMARY KEY(slice, projection_name, timestamp_offset, persistence_id, seq_nr)
      );
      CREATE TABLE IF NOT EXISTS akka_projection_management (
        projection_name VARCHAR(255) NOT NULL,
        projection_key VARCHAR(255) NOT NULL,
        paused BOOLEAN NOT NULL,
        last_updated BIGINT NOT NULL,
        PRIMARY KEY(projection_name, projection_key)
       );
     $tripleQuote

     akka.persistence.r2dbc {
       query {
         refresh-interval = 500 millis
         # reducing this to have quicker test, triggers backtracking earlier
         backtracking.behind-current-time = 3 seconds
       }
       journal.publish-events-number-of-topics = 2
     }

     # consumer uses its own h2
     test.consumer.r2dbc = $${akka.persistence.r2dbc}
     test.consumer.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
     test.consumer.r2dbc.connection-factory.database = "consumer-db"
     
     test.consumer.projection = $${akka.projection.r2dbc}
     test.consumer.projection.use-connection-factory = "test.consumer.r2dbc.connection-factory"
    """).withFallback(ConfigFactory.load("persistence.conf")).resolve()
}
class ProducerPushSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ProducerPushReplicationSpec",
          ProducerPushSpec.config
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {

  def this() = this(new TestContainerConf)
  override def typedSystem: ActorSystem[_] = system

  val entityType = nextEntityType()
  val streamId = "entity_stream_id"
  val producerProjectionId = randomProjectionId()
  val consumerProjectionId = randomProjectionId()

  lazy val consumerProjectionSettings: R2dbcProjectionSettings =
    R2dbcProjectionSettings(typedSystem.settings.config.getConfig("test.consumer.projection"))

  val grpcPort = 9588

  def producerSourceProvider(eps: EventProducerSource) =
    EventSourcedProvider.eventsBySlices[String](system, R2dbcReadJournal.Identifier, eps.entityType, 0, 1023)

  val eventConsumerClient = EventConsumerServiceClient(
    GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false))

  // this projection runs in the producer and pushes events over grpc to the consumer
  def spawnProducerReplicationProjection(eps: EventProducerSource): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[String]](
          producerProjectionId,
          settings = None,
          sourceProvider = producerSourceProvider(eps),
          handler = EventPusher(eventConsumerClient, eps))))

  def counsumerSourceProvider = {
    // FIXME how do we auto-wrap with this?
    new FilteredPayloadMapper(
      EventSourcedProvider.eventsBySlices[String](system, "test.consumer.r2dbc.query", entityType, 0, 1023))
  }

  // this projection runs in the consumer and just consumes the already projected events
  def spawnConsumerProjection(probe: ActorRef[EventEnvelope[String]]) =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          consumerProjectionId,
          settings = None,
          sourceProvider = counsumerSourceProvider,
          handler = () => {
            envelope: EventEnvelope[String] =>
              probe ! envelope
              Future.successful(Done)
          })))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // clean up consumer tables as well as producer tables (happens in super)
    lazy val consumerSettings: R2dbcSettings =
      R2dbcSettings(typedSystem.settings.config.getConfig("test.consumer.r2dbc"))
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${consumerSettings.journalTableWithSchema}")),
      10.seconds)
    Await.result(
      r2dbcExecutor.updateOne("beforeAll delete")(
        _.createStatement(s"delete from ${consumerProjectionSettings.timestampOffsetTableWithSchema}")),
      10.seconds)
  }

  "Producer pushed events" should {

    "show up on consumer side" in {
      // consumer runs gRPC server accepting pushed events from producers
      val bound = Http(system)
        .newServerAt("127.0.0.1", grpcPort)
        .bind(
          // FIXME higher level api doing both these steps and returning the handler?
          // FIXME how does it fit with the existing EPS-binding APIs?
          // FIXME auth should be possible just like the regular gRPC transport - we don't anyone to push events directly into the journal
          // FIXME consumer filters
          EventConsumerServiceHandler(
            // events are written directly into the journal on the consumer side, pushing over gRPC is only
            // allowed if no two pushing systems push events for the same persistence id
            EventConsumerServiceImpl
              .directJournalConsumer(
                journalPluginId = "test.consumer.r2dbc.journal",
                // FIXME we might want to allow transforming more aspects of the events (payloads even?)
                persistenceIdTransformer = identity,
                // FIXME or should it be entity type right away, rationale for separating entity type from stream id
                //       was that consumer shouldn't necessarily know the internal entity type string for a producer,
                //       but maybe less important when it is the producer doing the pushing?
                acceptedStreamIds = Set(streamId))))
      bound.futureValue

      // FIXME higher level API for the producer side of this?
      // FIXME producer filters
      val veggies = Set("cucumber")
      val eps =
        EventProducerSource[String](
          entityType,
          streamId,
          Transformation.identity,
          EventProducerSettings(system),
          producerFilter = envelope =>
            // no veggies allowed
            !veggies(envelope.event))

      spawnProducerReplicationProjection(eps)

      // local "regular" projections consume the projected events
      val consumerProbe = createTestProbe[EventEnvelope[String]]()

      // FIXME do we provide some higher level API for both accepting and consuming or do
      //       we leave that up to the user (so that they can decide on using SDP or whatnot?)
      spawnConsumerProjection(consumerProbe.ref)

      val pid = nextPid(entityType)
      // running this directly, as in producing system (even though they share actor system)
      // written in its own db, replicated over grpc to the consumer db.
      val entity1 = spawn(TestEntity(pid))
      entity1 ! TestEntity.Persist("bananas")
      entity1 ! TestEntity.Persist("cucumber") // producer filter - never seen in consumer
      entity1 ! TestEntity.Persist("mangos")

      // event projected into consumer journal and shows up in local projection
      consumerProbe.receiveMessage(10.seconds).event should be("bananas")
      // Note: filtered ends up in the consumer journal, but does not show up in projection
      consumerProbe.receiveMessage().event should be("mangos")

      val entity2 = spawn(TestEntity(nextPid(entityType)))
      val entity3 = spawn(TestEntity(nextPid(entityType)))
      for (i <- 0 to 100) {
        entity1 ! TestEntity.Persist(s"peach-$i-entity1")
        entity2 ! TestEntity.Persist(s"peach-$i-entity2")
        entity3 ! TestEntity.Persist(s"peach-$i-entity3")
      }
      consumerProbe.receiveMessages(300)
    }
  }

}
