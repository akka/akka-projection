/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.producer.EventProducerSettings
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

object PushReplicationMigrationIntegrationSpec {

  private def config(dc: ReplicaId): Config = {
    val journalTable = if (dc == ReplicaId.empty) "event_journal" else s"event_journal_${dc.id}"
    val timestampOffsetTable =
      if (dc == ReplicaId.empty) "akka_projection_timestamp_offset_store"
      else s"akka_projection_timestamp_offset_store_${dc.id}"
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.http.server.preview.enable-http2 = on
       akka.persistence.r2dbc {
          journal.table = "$journalTable"
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
          timestamp-offset-table = "$timestampOffsetTable"
        }
        akka.remote.artery.canonical.host = "127.0.0.1"
        akka.remote.artery.canonical.port = 0
        akka.actor.testkit.typed {
          filter-leeway = 10s
          system-shutdown-default = 30s
        }
      """)
  }

  private val DCA = ReplicaId("DCA")
  private val DCB = ReplicaId("DCB")
  private val EdgeReplicaA = ReplicaId.empty

}

class PushReplicationMigrationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationMigrationIntegrationSpecA",
          PushReplicationMigrationIntegrationSpec
            .config(PushReplicationMigrationIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import PushReplicationMigrationIntegrationSpec._
  import ReplicationMigrationIntegrationSpec.HelloWorld
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[PushReplicationMigrationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationMigrationIntegrationSpecB",
        PushReplicationMigrationIntegrationSpec
          .config(PushReplicationMigrationIntegrationSpec.DCB)
          .withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationMigrationIntegrationSpecEdgeA",
        PushReplicationMigrationIntegrationSpec
          .config(PushReplicationMigrationIntegrationSpec.EdgeReplicaA)
          .withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(2, "127.0.0.1").map(_.getPort)
  def grpcClientSettings(index: Int) =
    GrpcClientSettings.connectToServiceAt("127.0.0.1", grpcPorts(index)).withTls(false)
  private val replicaA = Replica(DCA, 2, grpcClientSettings(0))
  private val replicaB = Replica(DCB, 2, grpcClientSettings(1))
  private val allCloudReplicas = Set(replicaA, replicaB)

  /*
  private val _ = Replica(
    EdgeReplicaA,
    2,
    // Note: there is no way to actively connect to this replica, instead the GrpcClientSettings would be how _it_ connects
    // (to DCA in this case). The normal replicas does not have the Replica in their lists of all replicas
    replicaA.grpcClientSettings)
   */

  private val testKitsPerDc =
    Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)), EdgeReplicaA -> ActorTestKit(systems(2)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1), EdgeReplicaA -> systems(2))
  private var replicationA: Replication[HelloWorld.Command] = _
  private var replicationB: Replication[HelloWorld.Command] = _
  private var edgeReplicationA: EdgeReplication[HelloWorld.Command] = _
  private val entityIdOne = "one"
  private val entityIdTwo = "two"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], replica: Replica): Future[Replication[HelloWorld.Command]] = {
    logger
      .info(
        "Starting replica [{}], system [{}] on port [{}]",
        replica.replicaId,
        replicaSystem.name,
        replica.grpcClientSettings.defaultPort)

    val grpcPort = replica.grpcClientSettings.defaultPort
    val settings = ReplicationSettings[HelloWorld.Command](
      HelloWorld.EntityType,
      replica.replicaId,
      EventProducerSettings(replicaSystem),
      allCloudReplicas,
      10.seconds,
      8,
      R2dbcReplication()).withEdgeReplication(true)
    val started =
      Replication.grpcReplication(settings)(HelloWorld.replicated)(replicaSystem)

    // start producer server
    Http()(system)
      .newServerAt("127.0.0.1", grpcPort)
      .bind(started.createSingleServiceHandler())
      .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
      .map(_ => started)
  }

  def startEdgeReplica(
      replicaSystem: ActorSystem[_],
      selfReplicaId: ReplicaId,
      connectTo: Replica): EdgeReplication[HelloWorld.Command] = {
    val settings = ReplicationSettings[HelloWorld.Command](
      HelloWorld.EntityType,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      Set(connectTo),
      10.seconds,
      // few on edge node (but 2 rather than 1 here to make sure test actually covers parallel updates)
      2,
      R2dbcReplication()).withEdgeReplication(true)
    Replication.grpcEdgeReplication(settings)(HelloWorld.replicated)(replicaSystem)
  }

  "Replication over gRPC" should {
    "form one edge cluster" in {
      // Probably not realistic to start with non-replicated at edge, and then make it replicated,
      // but that will test the push aspect, which is most different compared to ReplicationMigrationIntegrationSpec
      val testKit = testKitsPerDc(EdgeReplicaA)
      val cluster = Cluster(testKit.system)
      cluster.manager ! Join(cluster.selfMember.address)
      testKit.createTestProbe().awaitAssert {
        cluster.selfMember.status should ===(MemberStatus.Up)
      }
    }

    "persist events with non-replicated EventSourcedBehavior" in {
      val testKit = testKitsPerDc(EdgeReplicaA)
      Set(entityIdOne, entityIdTwo).foreach { entityId =>
        withClue(s"for entity id $entityId") {
          val entity = testKit.spawn(HelloWorld.nonReplicated(PersistenceId.of(HelloWorld.EntityType.name, entityId)))

          import akka.actor.typed.scaladsl.AskPattern._
          entity.ask(HelloWorld.SetGreeting("hello 1 from non-replicated", _)).futureValue
          entity.ask(HelloWorld.SetGreeting("hello 2 from non-replicated", _)).futureValue
          testKit.stop(entity)
        }
      }
    }

    "form three one node clusters" in {
      testKitsPerDc.foreach {
        case (dc, testKit) =>
          if (dc != EdgeReplicaA) {
            val cluster = Cluster(testKit.system)
            cluster.manager ! Join(cluster.selfMember.address)
            testKit.createTestProbe().awaitAssert {
              cluster.selfMember.status should ===(MemberStatus.Up)
            }
          }
      }
    }

    "start two cloud replicas and one edge replica" in {
      replicationA = startReplica(systemPerDc(DCA), replicaA).futureValue
      replicationB = startReplica(systemPerDc(DCB), replicaB).futureValue
      edgeReplicationA = startEdgeReplica(systemPerDc(EdgeReplicaA), EdgeReplicaA, replicaA)
      logger.info("All three replication/producer services bound")
    }

    "recover from non-replicated events" in {
      testKitsPerDc.values.foreach { testKit =>
        withClue(s"on ${testKit.system.name}") {
          val probe = testKit.createTestProbe()

          Set(entityIdOne, entityIdTwo).foreach { entityId =>
            withClue(s"for entity id $entityId") {
              val entityRef = ClusterSharding(testKit.system)
                .entityRefFor(HelloWorld.EntityType, entityId)

              probe.awaitAssert({
                entityRef
                  .ask(HelloWorld.Get.apply)
                  .futureValue should ===("hello 2 from non-replicated")
              }, 10.seconds)
            }
          }
        }
      }
    }

    "replicate writes directly from cloud to edge" in {
      for {
        n <- 1 to 5
        entityId <- Set(entityIdOne, entityIdTwo)
      } {
        logger.info("Updating greeting {} for [{}] from dc [{}]", n, entityId, DCA)
        replicationA
          .entityRefFactory(entityId)
          .ask(HelloWorld.SetGreeting(s"hello $n from ${DCA.id}", _))
          .futureValue

        val edgeEntityRef = edgeReplicationA.entityRefFactory(entityId)
        val probe = testKit.createTestProbe()
        probe.awaitAssert({
          edgeEntityRef
            .ask(HelloWorld.Get.apply)
            .futureValue should ===(s"hello $n from ${DCA.id}")
        }, 10.seconds)

        // and also B ofc (unrelated to edge replication but for good measure)
        val dcBEntityRef = replicationB.entityRefFactory(entityId)
        probe.awaitAssert({
          dcBEntityRef
            .ask(HelloWorld.Get.apply)
            .futureValue should ===(s"hello $n from ${DCA.id}")
        }, 10.seconds)
      }
    }

    "replicate writes from edge node to cloud" in {
      for {
        n <- 6 to 10
        entityId <- Set(entityIdOne, entityIdTwo)
      } {
        logger.info("Updating greeting {} for [{}] from edge dc", n, entityId)
        edgeReplicationA
          .entityRefFactory(entityId)
          .ask(HelloWorld.SetGreeting(s"hello $n from edge", _))
          .futureValue

        val probe = testKit.createTestProbe()
        // should reach the direct replica
        val dcAEntityRef = replicationA.entityRefFactory(entityId)
        probe.awaitAssert({
          dcAEntityRef
            .ask(HelloWorld.Get.apply)
            .futureValue should ===(s"hello $n from edge")
        }, 10.seconds)

        // then indirectly replica B
        val dcBEntityRef = replicationB.entityRefFactory(entityId)
        probe.awaitAssert({
          dcBEntityRef
            .ask(HelloWorld.Get.apply)
            .futureValue should ===(s"hello $n from edge")
        }, 10.seconds)
      }
    }
  }

  protected override def afterAll(): Unit = {
    logger.info("Shutting down all three DCs")
    systems.foreach(_.terminate()) // speed up termination by terminating all at once
    // and then make sure they are completely shutdown
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}
