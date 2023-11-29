/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.Replication.EdgeReplication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.R2dbcProjectionSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object PushReplicationIntegrationSpec {

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.actor {
         serialization-bindings {
           "${classOf[replication.ReplicationIntegrationSpec].getName}$$LWWHelloWorld$$Event" = jackson-json
         }
       }
       akka.http.server.preview.enable-http2 = on
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

  private val DCA = ReplicaId("DCA")
  private val DCB = ReplicaId("DCB")
  private val EdgeReplicaA = ReplicaId("EdgeA")

}

class PushReplicationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationIntegrationSpecA",
          PushReplicationIntegrationSpec
            .config(PushReplicationIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import PushReplicationIntegrationSpec._
  import ReplicationIntegrationSpec.LWWHelloWorld
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[PushReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationIntegrationSpecB",
        PushReplicationIntegrationSpec
          .config(PushReplicationIntegrationSpec.DCB)
          .withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationIntegrationSpecEdgeA",
        PushReplicationIntegrationSpec
          .config(PushReplicationIntegrationSpec.EdgeReplicaA)
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
  private var replicationA: Replication[LWWHelloWorld.Command] = _
  private var replicationB: Replication[LWWHelloWorld.Command] = _
  private var edgeReplicationA: EdgeReplication[LWWHelloWorld.Command] = _
  private val entityIdOne = "one"

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    // We can share the journal to save a bit of work, because the persistence id contains
    // the dc so is unique (this is ofc completely synthetic, the whole point of replication
    // over grpc is to replicate between different dcs/regions with completely separate databases).
    // The offset tables need to be separate though to not get conflicts on projection names
    systemPerDc.values.foreach { system =>
      val r2dbcProjectionSettings = R2dbcProjectionSettings(system)
      Await.result(
        r2dbcExecutor.updateOne("beforeAll delete")(
          _.createStatement(s"delete from ${r2dbcProjectionSettings.timestampOffsetTableWithSchema}")),
        10.seconds)
    }
  }

  val EntityTypeName = "hello-edge-world"

  def startReplica(replicaSystem: ActorSystem[_], replica: Replica): Future[Replication[LWWHelloWorld.Command]] = {
    logger
      .infoN(
        "Starting replica [{}], system [{}] on port [{}]",
        replica.replicaId,
        replicaSystem.name,
        replica.grpcClientSettings.defaultPort)

    val grpcPort = replica.grpcClientSettings.defaultPort
    val settings = ReplicationSettings[LWWHelloWorld.Command](
      EntityTypeName,
      replica.replicaId,
      EventProducerSettings(replicaSystem),
      allCloudReplicas,
      10.seconds,
      8,
      R2dbcReplication()).withEdgeReplication(true)
    val started =
      Replication.grpcReplication(settings)(LWWHelloWorld.apply)(replicaSystem)

    // start producer server
    Http(system)
      .newServerAt("127.0.0.1", grpcPort)
      .bind(started.createSingleServiceHandler())
      .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
      .map(_ => started)
  }

  def startEdgeReplica(
      replicaSystem: ActorSystem[_],
      selfReplicaId: ReplicaId,
      connectTo: Replica): EdgeReplication[LWWHelloWorld.Command] = {
    val settings = ReplicationSettings[LWWHelloWorld.Command](
      EntityTypeName,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      Set(connectTo),
      10.seconds,
      8,
      R2dbcReplication()).withEdgeReplication(true)
    Replication.grpcEdgeReplication(settings)(LWWHelloWorld.apply)(replicaSystem)
  }

  "Replication over gRPC" should {
    "form three one node clusters" in {
      testKitsPerDc.values.foreach { testKit =>
        val cluster = Cluster(testKit.system)
        cluster.manager ! Join(cluster.selfMember.address)
        testKit.createTestProbe().awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
        }
      }
    }

    "start two cloud replicas and one edge replica" in {
      replicationA = startReplica(systemPerDc(DCA), replicaA).futureValue
      replicationB = startReplica(systemPerDc(DCB), replicaB).futureValue
      edgeReplicationA = startEdgeReplica(systemPerDc(EdgeReplicaA), EdgeReplicaA, replicaA)
      logger.info("All three replication/producer services bound")
    }

    "replicate writes directly from cloud to edge" in {
      logger.infoN("Updating greeting for [{}] from dc [{}]", entityIdOne, DCA)
      replicationA
        .entityRefFactory(entityIdOne)
        .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${DCA.id}", _))
        .futureValue

      val edgeEntityRef = edgeReplicationA.entityRefFactory(entityIdOne)
      val probe = testKit.createTestProbe()
      probe.awaitAssert({
        edgeEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${DCA.id}")
      }, 10.seconds)

      // and also B ofc (unrelated to edge replication but for good measure)
      val dcBEntityRef = replicationB.entityRefFactory(entityIdOne)
      probe.awaitAssert({
        dcBEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${DCA.id}")
      }, 10.seconds)
    }

    "replicate writes from edge node to cloud" in {
      logger.infoN("Updating greeting for [{}] from dc [{}]", entityIdOne, edgeReplicationA)
      edgeReplicationA
        .entityRefFactory(entityIdOne)
        .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${EdgeReplicaA.id}", _))
        .futureValue

      val probe = testKit.createTestProbe()
      // should reach the direct replica
      val dcAEntityRef = replicationA.entityRefFactory(entityIdOne)
      probe.awaitAssert({
        dcAEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${EdgeReplicaA.id}")
      }, 10.seconds)

      // then indirectly replica B
      val dcBEntityRef = replicationB.entityRefFactory(entityIdOne)
      probe.awaitAssert({
        dcBEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${EdgeReplicaA.id}")
      }, 10.seconds)

    }

    "replicate writes from one DCB to DCA and then the edge node" in {
      logger.infoN("Updating greeting for [{}] from dc [{}]", entityIdOne, DCB)
      replicationB
        .entityRefFactory(entityIdOne)
        .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${DCB.id}", _))
        .futureValue

      // should reach the other replica
      val dcAEntityRef = replicationA.entityRefFactory(entityIdOne)
      val probe = testKit.createTestProbe()
      probe.awaitAssert({
        dcAEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${DCB.id}")
      }, 10.seconds)

      // then edge
      val edgeEntityRef = edgeReplicationA.entityRefFactory(entityIdOne)
      probe.awaitAssert({
        edgeEntityRef
          .ask(LWWHelloWorld.Get.apply)
          .futureValue should ===(s"hello 1 from ${DCB.id}")
      }, 10.seconds)

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
