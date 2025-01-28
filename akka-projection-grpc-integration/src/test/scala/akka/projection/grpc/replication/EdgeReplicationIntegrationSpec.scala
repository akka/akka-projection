/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.Done
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
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.IncludeTags
import akka.projection.grpc.consumer.ConsumerFilter.UpdateFilter
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

object EdgeReplicationIntegrationSpec {

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
  private val CloudReplicaB = ReplicaId("DCB")
  private val EdgeReplicaC = ReplicaId("DCC")
  private val EdgeReplicaD = ReplicaId("DCD")

}

class EdgeReplicationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "EdgeReplicationIntegrationSpecA",
          EdgeReplicationIntegrationSpec
            .config(EdgeReplicationIntegrationSpec.CloudReplicaA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import EdgeReplicationIntegrationSpec._
  import ReplicationIntegrationSpec.LWWHelloWorld
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[EdgeReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationSpecB",
        EdgeReplicationIntegrationSpec.config(CloudReplicaB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationSpecC",
        EdgeReplicationIntegrationSpec.config(EdgeReplicaC).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationSpecD",
        EdgeReplicationIntegrationSpec.config(EdgeReplicaD).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(CloudReplicaA, CloudReplicaB, EdgeReplicaC, EdgeReplicaD).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }.toSet

  private val testKitsPerDc = Map(
    CloudReplicaA -> testKit,
    CloudReplicaB -> ActorTestKit(systems(1)),
    EdgeReplicaC -> ActorTestKit(systems(2)),
    EdgeReplicaD -> ActorTestKit(systems(3)))
  private val systemPerDc =
    Map(CloudReplicaA -> system, CloudReplicaB -> systems(1), EdgeReplicaC -> systems(2), EdgeReplicaD -> systems(3))
  private val entityIds = Set(
    nextPid(LWWHelloWorld.EntityType.name).entityId,
    nextPid(LWWHelloWorld.EntityType.name).entityId,
    nextPid(LWWHelloWorld.EntityType.name).entityId)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[LWWHelloWorld.Command] = {
    def replicationSettings(otherReplicas: Set[Replica]) = {
      ReplicationSettings[LWWHelloWorld.Command](
        LWWHelloWorld.EntityType.name,
        selfReplicaId,
        EventProducerSettings(replicaSystem),
        otherReplicas,
        10.seconds,
        8,
        R2dbcReplication()(replicaSystem))
        .withEdgeReplication(true)
    }

    selfReplicaId match {
      case CloudReplicaA =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaB)
        Replication.grpcReplication(replicationSettings(otherReplicas))(LWWHelloWorld.apply)(replicaSystem)

      case CloudReplicaB =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcReplication(replicationSettings(otherReplicas))(LWWHelloWorld.apply)(replicaSystem)

      case other =>
        throw new IllegalArgumentException(other.id)
    }
  }

  def startEdgeReplica(
      replicaSystem: ActorSystem[_],
      selfReplicaId: ReplicaId): EdgeReplication[LWWHelloWorld.Command] = {
    def replicationSettings(otherReplicas: Set[Replica]) = {
      ReplicationSettings[LWWHelloWorld.Command](
        LWWHelloWorld.EntityType.name,
        selfReplicaId,
        EventProducerSettings(replicaSystem),
        otherReplicas,
        10.seconds,
        8,
        R2dbcReplication()(replicaSystem))
    }

    selfReplicaId match {
      case EdgeReplicaC =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcEdgeReplication(replicationSettings(otherReplicas))(LWWHelloWorld.apply)(replicaSystem)

      case EdgeReplicaD =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcEdgeReplication(replicationSettings(otherReplicas))(LWWHelloWorld.apply)(replicaSystem)

      case other =>
        throw new IllegalArgumentException(other.id)
    }
  }

  def assertGreeting(entityId: String, expected: String): Unit = {
    testKitsPerDc.values.foreach { testKit =>
      withClue(s"on ${testKit.system.name}") {
        val probe = testKit.createTestProbe()
        withClue(s"for entity id $entityId") {
          val entityRef = ClusterSharding(testKit.system)
            .entityRefFor(LWWHelloWorld.EntityType, entityId)

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
      val replicasStarted = Future.sequence(allReplicas.zipWithIndex.map {
        case (replica, index) =>
          val system = systems(index)
          logger
            .info(
              "Starting replica [{}], system [{}] on port [{}]",
              replica.replicaId,
              system.name,
              replica.grpcClientSettings.defaultPort)
          if (replica.replicaId == CloudReplicaA || replica.replicaId == CloudReplicaB) {

            val started = startReplica(system, replica.replicaId)
            val grpcPort = grpcPorts(index)

            // start producer server
            Http()(system)
              .newServerAt("127.0.0.1", grpcPort)
              .bind(started.createSingleServiceHandler())
              .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
              .map(_ => Done)
          } else {
            startEdgeReplica(system, replica.replicaId)
            Future.successful(Done)
          }
      })

      replicasStarted.futureValue
      logger.info("All replication/producer services bound")
    }

    "replicate directly" in {
      val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

      ClusterSharding(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A1", _))
        .futureValue
      assertGreeting(entityId, "Hello from A1")

      ClusterSharding(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A2", _))
        .futureValue
      assertGreeting(entityId, "Hello from A2")
    }

    "replicate indirectly" in {
      val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

      // Edge replicas are only connected to CloudReplicaA
      ClusterSharding(systemPerDc(CloudReplicaB))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from B1", _))
        .futureValue
      assertGreeting(entityId, "Hello from B1")

      ClusterSharding(systemPerDc(CloudReplicaB))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from B2", _))
        .futureValue
      assertGreeting(entityId, "Hello from B2")
    }

    "replicate both directions" in {
      val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

      ClusterSharding(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A1", _))
        .futureValue
      assertGreeting(entityId, "Hello from A1")

      ClusterSharding(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from C1", _))
        .futureValue
      assertGreeting(entityId, "Hello from C1")

      ClusterSharding(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A2", _))
        .futureValue
      assertGreeting(entityId, "Hello from A2")

      ClusterSharding(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from C2", _))
        .futureValue
      assertGreeting(entityId, "Hello from C2")
    }

    "replicate writes from one dc to the other DCs" in {
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          Future
            .sequence(entityIds.map { entityId =>
              logger.info("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
              ClusterSharding(systemPerDc(dc))
                .entityRefFor(LWWHelloWorld.EntityType, entityId)
                .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${dc.id}", _))
            })
            .futureValue

          testKitsPerDc.values.foreach { testKit =>
            withClue(s"on ${testKit.system.name}") {
              val probe = testKit.createTestProbe()

              entityIds.foreach { entityId =>
                withClue(s"for entity id $entityId") {
                  val entityRef = ClusterSharding(testKit.system)
                    .entityRefFor(LWWHelloWorld.EntityType, entityId)

                  probe.awaitAssert({
                    entityRef
                      .ask(LWWHelloWorld.Get.apply)
                      .futureValue should ===(s"hello 1 from ${dc.id}")
                  }, 10.seconds)
                }
              }
            }
          }
        }
      }
    }

    "replicate concurrent writes to the other DCs" in (2 to 4).foreach { greetingNo =>
      withClue(s"Greeting $greetingNo") {
        Future
          .sequence(systemPerDc.keys.map { dc =>
            withClue(s"from ${dc.id}") {
              Future.sequence(entityIds.map { entityId =>
                logger.info("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
                ClusterSharding(systemPerDc(dc))
                  .entityRefFor(LWWHelloWorld.EntityType, entityId)
                  .ask(LWWHelloWorld.SetGreeting(s"hello $greetingNo from ${dc.id}", _))
              })
            }
          })
          .futureValue // all updated in roughly parallel

        // All 3 should eventually arrive at the same value
        testKit
          .createTestProbe()
          .awaitAssert(
            {
              entityIds.foreach { entityId =>
                withClue(s"for entity id $entityId") {
                  testKitsPerDc.values.map { testKit =>
                    val entityRef = ClusterSharding(testKit.system)
                      .entityRefFor(LWWHelloWorld.EntityType, entityId)

                    entityRef
                      .ask(LWWHelloWorld.Get.apply)
                      .futureValue
                  }.toSet should have size (1)
                }
              }
            },
            20.seconds)
      }
    }
  }

  "use consumer filter on tag" in {
    val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

    ConsumerFilter(systemPerDc(EdgeReplicaC)).ref ! UpdateFilter(
      LWWHelloWorld.EntityType.name,
      List(ConsumerFilter.excludeAll, IncludeTags(Set("tag-C"))))
    ConsumerFilter(systemPerDc(EdgeReplicaD)).ref ! UpdateFilter(
      LWWHelloWorld.EntityType.name,
      List(ConsumerFilter.excludeAll, IncludeTags(Set("tag-D"))))

    // let the filter propagate to producer
    Thread.sleep(1000)

    ClusterSharding(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetTag("tag-C", _))
      .futureValue

    ClusterSharding(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetGreeting("Hello C", _))
      .futureValue

    eventually {
      ClusterSharding(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.Get(_))
        .futureValue shouldBe "Hello C"
    }

    // but not updated in D
    ClusterSharding(systemPerDc(EdgeReplicaD))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.Get(_))
      .futureValue shouldBe "Hello world"

    // change tag
    ClusterSharding(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetTag("tag-D", _))
      .futureValue

    // previous greeting should be replicated
    eventually {
      ClusterSharding(systemPerDc(EdgeReplicaD))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.Get(_))
        .futureValue shouldBe "Hello C"
    }

    ClusterSharding(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetGreeting("Hello D", _))
      .futureValue
    eventually {
      ClusterSharding(systemPerDc(EdgeReplicaD))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.Get(_))
        .futureValue shouldBe "Hello D"
    }

    // but not updated in C
    ClusterSharding(systemPerDc(EdgeReplicaC))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.Get(_))
      .futureValue shouldBe "Hello C"
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
