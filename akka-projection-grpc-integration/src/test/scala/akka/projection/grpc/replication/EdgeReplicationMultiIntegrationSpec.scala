/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.replication.ReplicationIntegrationSpec.LWWHelloWorld.Command
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.Replication.EdgeReplication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

/**
 * Covers setting up multiple RES entity types between one edge and one cloud replica
 */
object EdgeReplicationMultiIntegrationSpec {

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.actor {
         serialization-bindings {
           "${classOf[ReplicationIntegrationSpec].getName}$$LWWHelloWorld$$Event" = jackson-json
         }
       }
       akka.http.server.preview.enable-http2 = on
       akka.persistence.r2dbc {
          journal.table = "event_journal_${dc.id}"
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
        akka.projection.r2dbc.offset-store {
          timestamp-offset-table = "akka_projection_timestamp_offset_store_${dc.id}"
        }
        akka.remote.artery.canonical.host = "127.0.0.1"
        akka.remote.artery.canonical.port = 0
        akka.actor.testkit.typed {
          filter-leeway = 10s
          system-shutdown-default = 30s
        }
      """)

  private val CloudReplicaA = ReplicaId("DCA")
  private val EdgeReplicaC = ReplicaId("DCC")

  // re-use the same RES impl but pretend it is a different type of RES
  object LWWHelloWorld2 {
    val EntityType: EntityTypeKey[Command] = EntityTypeKey[Command]("hello-world2")
  }
}

class EdgeReplicationMultiIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "EdgeReplicationMultiIntegrationSpecA",
          EdgeReplicationMultiIntegrationSpec
            .config(EdgeReplicationMultiIntegrationSpec.CloudReplicaA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import EdgeReplicationMultiIntegrationSpec._
  import ReplicationIntegrationSpec.LWWHelloWorld
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[EdgeReplicationMultiIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val cloudReplicaSystem = typedSystem
  private val edgeSystem = akka.actor
    .ActorSystem(
      "EdgeReplicationMultiIntegrationSpecC",
      EdgeReplicationMultiIntegrationSpec.config(EdgeReplicaC).withFallback(testContainerConf.config))
    .toTyped

  private val systems = Seq[ActorSystem[_]](typedSystem, edgeSystem)

  private val cloudReplicaPort = SocketUtil.temporaryServerAddress("127.0.0.1")
  private val cloudReplica = Replica(
    CloudReplicaA,
    1,
    GrpcClientSettings.connectToServiceAt("127.0.0.1", cloudReplicaPort.getPort).withTls(false))

  private val testKitsPerDc = Map(CloudReplicaA -> testKit, EdgeReplicaC -> ActorTestKit(edgeSystem))
  private val systemPerDc = Map(CloudReplicaA -> system, EdgeReplicaC -> edgeSystem)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startCloudReplica(): (Replication[LWWHelloWorld.Command], Replication[LWWHelloWorld.Command]) = {

    def replicaSettings(entityTypeKey: EntityTypeKey[LWWHelloWorld.Command]) =
      ReplicationSettings[LWWHelloWorld.Command](
        entityTypeKey.name,
        CloudReplicaA,
        EventProducerSettings(cloudReplicaSystem),
        Set.empty[Replica],
        10.seconds,
        2,
        R2dbcReplication())
        .withEdgeReplication(true)

    val replication1 =
      Replication.grpcReplication(replicaSettings(LWWHelloWorld.EntityType))(LWWHelloWorld.apply)(cloudReplicaSystem)
    val replication2 =
      Replication.grpcReplication(replicaSettings(LWWHelloWorld2.EntityType))(LWWHelloWorld.apply)(cloudReplicaSystem)

    // this is the tricky part, binding both these into listening services
    (replication1, replication2)
  }

  def startEdgeReplica(): (EdgeReplication[LWWHelloWorld.Command], EdgeReplication[LWWHelloWorld.Command]) = {
    def replicationSettings(entityTypeKey: EntityTypeKey[LWWHelloWorld.Command]) = {
      ReplicationSettings[LWWHelloWorld.Command](
        entityTypeKey.name,
        EdgeReplicaC,
        EventProducerSettings(edgeSystem),
        Set(cloudReplica),
        10.seconds,
        1,
        R2dbcReplication())
    }

    val replication1 =
      Replication.grpcEdgeReplication(replicationSettings(LWWHelloWorld.EntityType))(LWWHelloWorld.apply)(edgeSystem)
    val replication2 =
      Replication.grpcEdgeReplication(replicationSettings(LWWHelloWorld2.EntityType))(LWWHelloWorld.apply)(edgeSystem)

    // this one is straightforward, no gRPC services bound, only locally running sharding and event push-projections
    (replication1, replication2)
  }

  def assertGreeting(entityTypeKey: EntityTypeKey[LWWHelloWorld.Command], entityId: String, expected: String): Unit = {
    testKitsPerDc.values.foreach { testKit =>
      withClue(s"on ${testKit.system.name}") {
        val probe = testKit.createTestProbe()
        withClue(s"for entity id $entityId") {
          val entityRef = ClusterSharding(testKit.system)
            .entityRefFor(entityTypeKey, entityId)

          probe.awaitAssert({
            entityRef
              .ask(LWWHelloWorld.Get.apply)
              .futureValue should ===(expected)
          }, 10.seconds)
        }
      }
    }
  }

  "Replication over gRPC" should {
    "form one node clusters" in {
      testKitsPerDc.values.foreach { testKit =>
        val cluster = Cluster(testKit.system)
        cluster.manager ! Join(cluster.selfMember.address)
        testKit.createTestProbe().awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
        }
      }
    }

    "start replicas" in {
      logger
        .info(
          "Starting replica [{}], system [{}] on port [{}]",
          cloudReplica.replicaId,
          system.name,
          cloudReplica.grpcClientSettings.defaultPort)

      val (replication1, replication2) = startCloudReplica()
      val grpcPort = cloudReplica.grpcClientSettings.defaultPort

      // same Akka Projection gRPC for both entities for the edge pulling events
      val consumeHandler =
        EventProducer.grpcServiceHandler(Set(replication1.eventProducerSource, replication2.eventProducerSource))(
          system)

      // same Akka Projection gRPC service for both entities for the edge pushing events
      // FIXME annoying that it is an option, could we type our way around it somehow?
      val produceHandler = EventProducerPushDestination.grpcServiceHandler(
        Set(replication1.eventProducerPushDestination, replication2.eventProducerPushDestination).flatten)(system)

      val combinedHandler = consumeHandler.orElse(produceHandler)

      // start producer server
      Http()(system)
        .newServerAt("127.0.0.1", grpcPort)
        .bind(combinedHandler)
        .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
        .futureValue

      startEdgeReplica()
      logger.info("All replication/producer services bound")
    }

    "replicate directly" in {
      // simple verification of replication in both directions only,
      // more extensive replication tests in other specs
      val entity1Id = nextPid(LWWHelloWorld.EntityType.name).entityId

      ClusterSharding(cloudReplicaSystem)
        .entityRefFor(LWWHelloWorld.EntityType, entity1Id)
        .ask(LWWHelloWorld.SetGreeting("Hello from Cloud 1", _))
        .futureValue
      assertGreeting(LWWHelloWorld.EntityType, entity1Id, "Hello from Cloud 1")

      // the second entity type
      val entity2Id = nextPid(LWWHelloWorld2.EntityType.name).entityId
      ClusterSharding(cloudReplicaSystem)
        .entityRefFor(LWWHelloWorld2.EntityType, entity2Id)
        .ask(LWWHelloWorld.SetGreeting("Hello from Cloud 2", _))
        .futureValue
      assertGreeting(LWWHelloWorld2.EntityType, entity2Id, "Hello from Cloud 2")

      ClusterSharding(edgeSystem)
        .entityRefFor(LWWHelloWorld.EntityType, entity1Id)
        .ask(LWWHelloWorld.SetGreeting("Hello from Edge 1", _))
        .futureValue
      assertGreeting(LWWHelloWorld.EntityType, entity1Id, "Hello from Edge 1")

      // the second entity type
      ClusterSharding(cloudReplicaSystem)
        .entityRefFor(LWWHelloWorld2.EntityType, entity2Id)
        .ask(LWWHelloWorld.SetGreeting("Hello from Edge 2", _))
        .futureValue
      assertGreeting(LWWHelloWorld2.EntityType, entity2Id, "Hello from Edge 2")
    }
  }

  protected override def afterAll(): Unit = {
    logger.info("Shutting down all DCs")
    systems.foreach(_.terminate()) // speed up termination by terminating all at the once
    // and then make sure they are completely shutdown
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}
