/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.replication

import akka.Done
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.projection.ProjectionBehavior
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.TestEntity
import akka.projection.grpc.internal.EventConsumerServiceImpl
import akka.projection.grpc.internal.EventPusher
import akka.projection.grpc.internal.proto.EventConsumerServiceClient
import akka.projection.grpc.internal.proto.EventConsumerServiceHandler
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Future
import scala.concurrent.duration._

object ProducerPushReplicationSpec {

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

     # consumer uses its own h2
     test.consumer.r2dbc = $${akka.persistence.r2dbc}
     test.consumer.r2dbc.connection-factory = $${akka.persistence.r2dbc.h2}
     test.consumer.r2dbc.connection-factory.database = "consumer-db"
    """).withFallback(ConfigFactory.load("persistence.conf")).resolve()
}
class ProducerPushReplicationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ProducerPushReplicationSpec",
          ProducerPushReplicationSpec.config
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
  val producerProjectionId = randomProjectionId()
  val consumerProjectionId = randomProjectionId()

  val grpcPort = 9588

  def producerSourceProvider =
    EventSourcedProvider.eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityType, 0, 1023)

  val eventConsumerClient = EventConsumerServiceClient(
    GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPort).withTls(false))

  // this projection runs in the producer and pushes events over grpc to the consumer
  def spawnProducerReplicationProjection(): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          producerProjectionId,
          settings = None,
          sourceProvider = producerSourceProvider,
          handler = () => new EventPusher[String](eventConsumerClient, Transformation.identity))))

  def counsumerSourceProvider =
    EventSourcedProvider.eventsBySlices[String](system, "test.consumer.r2dbc.query", entityType, 0, 1023)

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

  "Producer pushed events" should {

    "show up on consumer side" in {
      // producer runs gRPC server accepting pushed events
      val bound = Http(system)
        .newServerAt("127.0.0.1", grpcPort)
        .bind(
          EventConsumerServiceHandler(
            // events are written directly into the journal on the consumer side, pushing over gRPC is only
            // allowed if no two pushing systems push events for the same persistence id
            EventConsumerServiceImpl.directJournalConsumer("test.consumer.r2dbc.journal")))
      bound.futureValue

      spawnProducerReplicationProjection()

      // local "regular" projections consume the projected events
      val consumerProbe = createTestProbe[EventEnvelope[String]]()
      spawnConsumerProjection(consumerProbe.ref)

      val pid = nextPid(entityType)
      // running this directly, as in producing system (even though they share actor system)
      // written in its own db, replicated over grpc to the consumer db.
      val entity = spawn(TestEntity(pid))
      entity ! TestEntity.Persist("bananas")

      // event projected into consumer journal and shows up in local projection
      consumerProbe.receiveMessage(10.seconds).event should be("bananas")
    }
  }

}
