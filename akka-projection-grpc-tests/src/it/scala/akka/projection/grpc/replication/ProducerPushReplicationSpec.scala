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
import akka.persistence.r2dbc.query.javadsl.R2dbcReadJournal
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
  def config = ConfigFactory.parseString("")
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
  val projectionId = randomProjectionId()

  def sourceProvider =
    EventSourcedProvider.eventsBySlices[String](system, R2dbcReadJournal.Identifier, entityType, 0, 1023)

  val eventConsumerClient = EventConsumerServiceClient(
    GrpcClientSettings.connectToServiceAt("localhost", 9090).withTls(false))

  def spawnPusherProjection(): ActorRef[ProjectionBehavior.Command] =
    spawn(
      ProjectionBehavior(
        R2dbcProjection.atLeastOnceAsync(
          projectionId,
          settings = None,
          sourceProvider = sourceProvider,
          handler = () => new EventPusher[String](eventConsumerClient, Transformation.identity))))

  "Producer pushed events" should {

    "show up on consumer side" in {
      // start producer server
      val consumerProbe = createTestProbe[EventEnvelope[Any]]()
      val bound = Http(system)
        .newServerAt("127.0.0.1", 9090)
        .bind(EventConsumerServiceHandler(new EventConsumerServiceImpl({ envelope =>
          // FIXME reacting to the push goes in internals
          consumerProbe.ref ! envelope
          Future.successful(Done)
        })(system)))
      bound.futureValue

      spawnPusherProjection()

      val pid = nextPid(entityType)
      // running this directly, as in producing system (even though they share actor system)
      val entity = spawn(TestEntity(pid))
      entity ! TestEntity.Persist("bananas")

      consumerProbe.receiveMessage(5.seconds).event should be("bananas")
    }
  }

}
