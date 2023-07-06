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
import akka.grpc.GrpcServiceException
import akka.grpc.scaladsl.MetadataBuilder
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
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.producer.scaladsl.EventProducerPush
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import com.typesafe.config.ConfigFactory
import io.grpc.Status
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

object EventProducerPushSpec {

  def config =
    ConfigFactory
      .parseString(s"""
    akka.http.server.enable-http2 = on

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

    # until present in reference.conf
    akka.persistence.typed.event-writer.max-batch-size=40
    akka.persistence.typed.event-writer.ask-timeout=5s
   """)
      .withFallback(ConfigFactory.load("application-h2.conf"))
      .withFallback(ConfigFactory.load())
      .resolve()
}
class EventProducerPushSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "EventProducerPushSpec",
          EventProducerPushSpec.config
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

  // this projection runs in the producer and pushes events over grpc to the consumer
  def spawnProducerReplicationProjection(
      eventProducer: EventProducerPush[String]): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceFlow[Offset, EventEnvelope[String]](
          producerProjectionId,
          settings = None,
          sourceProvider = EventSourcedProvider.eventsBySlices[String](
            system,
            // FIXME: could we use eventProducer.eventProducerSource.settings.queryPluginId (user would have to configure)
            R2dbcReadJournal.Identifier,
            eventProducer.eventProducerSource.entityType,
            0,
            1023),
          handler = eventProducer.handler())))

  // this projection runs in the consumer and just consumes the already projected events
  def spawnConsumerProjection(probe: ActorRef[EventEnvelope[String]]) =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          consumerProjectionId,
          settings = None,
          sourceProvider =
            EventSourcedProvider.eventsBySlices[String](system, "test.consumer.r2dbc.query", entityType, 0, 1023),
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
      val producerOriginId = "producer1"
      val consumerFilterExcludedPid = nextPid(entityType)

      // consumer runs gRPC server accepting pushed events from producers
      val destination =
        EventProducerPushDestination(streamId)
          .withJournalPluginId("test.consumer.r2dbc.journal")
          .withInterceptor((_, metadata) =>
            if (metadata.getText("secret").contains("password")) Future.successful(Done)
            else throw new GrpcServiceException(Status.PERMISSION_DENIED))
          .withTransformationForOrigin { (originId, _) =>
            EventProducerPushDestination.Transformation.empty
            // since filters touch different aspects of the events, they can be chained for the same type
              .registerPersistenceIdMapper(envelope => envelope.persistenceId.replace("p-", s"$originId-"))
              .registerTagMapper[String](_ => Set("added-tag"))
              .registerPayloadMapper[String, String](env => env.eventOption.map(_.toUpperCase))
          }
          .withConsumerFilters(Vector(ConsumerFilter.ExcludeEntityIds(Set(consumerFilterExcludedPid.id))))

      val bound = Http(system)
        .newServerAt("127.0.0.1", grpcPort)
        .bind(
          // events are written directly into the journal on the consumer side, pushing over gRPC is only
          // allowed if no two pushing systems push events for the same persistence id
          EventProducerPushDestination.grpcServiceHandler(destination))
      bound.futureValue

      val veggies = Set("cucumber")
      val authMetadata = (new MetadataBuilder).addText("secret", "password").build()
      val eventProducer = EventProducerPush[String](
        originId = producerOriginId,
        eventProducerSource = EventProducerSource[String](
          entityType,
          streamId,
          Transformation.identity,
          EventProducerSettings(system),
          // no veggies allowed
          producerFilter = envelope => !veggies(envelope.event)),
        connectionMetadata = authMetadata,
        GrpcClientSettings.connectToServiceAt("localhost", grpcPort).withTls(false))
      spawnProducerReplicationProjection(eventProducer)

      // local "regular" projections consume the projected events
      val consumerProbe = createTestProbe[EventEnvelope[String]]()
      spawnConsumerProjection(consumerProbe.ref)

      val pid = nextPid(entityType)
      // running this directly, as in producing system (even though they share actor system)
      // written in its own db, replicated over grpc to the consumer db.
      val entity1 = spawn(TestEntity(pid))
      entity1 ! TestEntity.Persist("bananas")
      entity1 ! TestEntity.Persist("cucumber") // producer filter - never seen in consumer
      entity1 ! TestEntity.Persist("mangos")

      // event projected into consumer journal and shows up in local projection
      val first = consumerProbe.receiveMessage(10.seconds)
      first.event should be("BANANAS")
      first.persistenceId should include(producerOriginId)
      first.tags should ===(Set("added-tag"))
      // Note: producer filtered cucumber ends up in the consumer journal, but does not show up in projection
      consumerProbe.receiveMessage().event should be("MANGOS")

      val consumerExcludedEntity = spawn(TestEntity(consumerFilterExcludedPid))
      // Note: consumer filtered guarana ends up in the consumer journal, but does not show up in projection
      consumerExcludedEntity ! TestEntity.Persist(s"cumquat")

      val entity2 = spawn(TestEntity(nextPid(entityType)))
      val entity3 = spawn(TestEntity(nextPid(entityType)))
      for (i <- 0 to 100) {
        entity1 ! TestEntity.Persist(s"peach-$i-entity1")
        entity2 ! TestEntity.Persist(s"peach-$i-entity2")
        entity3 ! TestEntity.Persist(s"peach-$i-entity3")
      }
      consumerProbe.receiveMessages(300, 20.seconds).foreach(envelope => envelope.event should include("PEACH"))
    }
  }

}
