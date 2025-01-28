/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.javadsl.Http
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.ConsumerFilter.IncludeTags
import akka.projection.grpc.consumer.ConsumerFilter.UpdateFilter
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.javadsl.EdgeReplication
import akka.projection.grpc.replication.javadsl.Replica
import akka.projection.grpc.replication.javadsl.Replication
import akka.projection.grpc.replication.javadsl.ReplicationSettings
import akka.projection.r2dbc.javadsl.R2dbcReplication
import akka.testkit.SocketUtil
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.time.Duration
import scala.jdk.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

object EdgeReplicationJavaDSLIntegrationSpec {

  private val CloudReplicaA = ReplicaId("DCA")
  private val CloudReplicaB = ReplicaId("DCB")
  private val EdgeReplicaC = ReplicaId("DCC")
  private val EdgeReplicaD = ReplicaId("DCD")

}

class EdgeReplicationJavaDSLIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "EdgeReplicationIntegrationJavaSpecA",
          ReplicationJavaDSLIntegrationSpec
            .config(EdgeReplicationJavaDSLIntegrationSpec.CloudReplicaA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import EdgeReplicationJavaDSLIntegrationSpec._
  import ReplicationJavaDSLIntegrationSpec.LWWHelloWorld
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[EdgeReplicationJavaDSLIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val askTimeout = Duration.ofSeconds(3)

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationJavaSpecB",
        ReplicationJavaDSLIntegrationSpec.config(CloudReplicaB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationJavaSpecC",
        ReplicationJavaDSLIntegrationSpec.config(EdgeReplicaC).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "EdgeReplicationIntegrationJavaSpecD",
        ReplicationJavaDSLIntegrationSpec.config(EdgeReplicaD).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(CloudReplicaA, CloudReplicaB, EdgeReplicaC, EdgeReplicaD).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica.create(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
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
      ReplicationSettings
        .create(
          classOf[LWWHelloWorld.Command],
          LWWHelloWorld.EntityType.name,
          selfReplicaId,
          EventProducerSettings.create(replicaSystem),
          otherReplicas.asJava,
          Duration.ofSeconds(10),
          8,
          R2dbcReplication.create(system))
        .withEdgeReplication(true)
    }

    selfReplicaId match {
      case CloudReplicaA =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaB)
        Replication.grpcReplication(replicationSettings(otherReplicas), LWWHelloWorld.create _, replicaSystem)

      case CloudReplicaB =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcReplication(replicationSettings(otherReplicas), LWWHelloWorld.create _, replicaSystem)

      case other =>
        throw new IllegalArgumentException(other.id)
    }
  }

  def startEdgeReplica(
      replicaSystem: ActorSystem[_],
      selfReplicaId: ReplicaId): EdgeReplication[LWWHelloWorld.Command] = {
    def replicationSettings(otherReplicas: Set[Replica]) = {
      ReplicationSettings.create(
        classOf[LWWHelloWorld.Command],
        LWWHelloWorld.EntityType.name,
        selfReplicaId,
        EventProducerSettings.create(replicaSystem),
        otherReplicas.asJava,
        Duration.ofSeconds(10),
        8,
        R2dbcReplication.create(replicaSystem))
    }

    selfReplicaId match {
      case EdgeReplicaC =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcEdgeReplication(replicationSettings(otherReplicas), LWWHelloWorld.create _, replicaSystem)

      case EdgeReplicaD =>
        val otherReplicas = allReplicas.filter(_.replicaId == CloudReplicaA)
        Replication.grpcEdgeReplication(replicationSettings(otherReplicas), LWWHelloWorld.create _, replicaSystem)

      case other =>
        throw new IllegalArgumentException(other.id)
    }
  }

  def assertGreeting(entityId: String, expected: String): Unit = {
    testKitsPerDc.values.foreach { testKit =>
      withClue(s"on ${testKit.system.name}") {
        val probe = testKit.createTestProbe()
        withClue(s"for entity id $entityId") {
          val entityRef = ClusterSharding
            .get(testKit.system)
            .entityRefFor(LWWHelloWorld.EntityType, entityId)

          probe.awaitAssert({
            entityRef
              .ask(LWWHelloWorld.Get(_), askTimeout)
              .toCompletableFuture
              .asScala
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
            Http
              .get(system)
              .newServerAt("127.0.0.1", grpcPort)
              .bind(started.createSingleServiceHandler())
              .asScala
              .map(_.addToCoordinatedShutdown(Duration.ofSeconds(3), system))(system.executionContext)
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

      ClusterSharding
        .get(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A1", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from A1")

      ClusterSharding
        .get(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A2", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from A2")
    }

    "replicate indirectly" in {
      val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

      // Edge replicas are only connected to CloudReplicaA
      ClusterSharding
        .get(systemPerDc(CloudReplicaB))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from B1", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from B1")

      ClusterSharding
        .get(systemPerDc(CloudReplicaB))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from B2", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from B2")
    }

    "replicate both directions" in {
      val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

      ClusterSharding
        .get(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A1", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from A1")

      ClusterSharding
        .get(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from C1", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from C1")

      ClusterSharding
        .get(systemPerDc(CloudReplicaA))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from A2", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from A2")

      ClusterSharding
        .get(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.SetGreeting("Hello from C2", _), askTimeout)
        .asScala
        .futureValue
      assertGreeting(entityId, "Hello from C2")
    }

    "replicate writes from one dc to the other DCs" in {
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          Future
            .sequence(entityIds.map { entityId =>
              logger.info("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
              ClusterSharding
                .get(systemPerDc(dc))
                .entityRefFor(LWWHelloWorld.EntityType, entityId)
                .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${dc.id}", _), askTimeout)
                .asScala
            })
            .futureValue

          testKitsPerDc.values.foreach { testKit =>
            withClue(s"on ${testKit.system.name}") {
              val probe = testKit.createTestProbe()

              entityIds.foreach { entityId =>
                withClue(s"for entity id $entityId") {
                  val entityRef = ClusterSharding
                    .get(testKit.system)
                    .entityRefFor(LWWHelloWorld.EntityType, entityId)

                  probe.awaitAssert({
                    entityRef
                      .ask(LWWHelloWorld.Get.apply, askTimeout)
                      .asScala
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
                ClusterSharding
                  .get(systemPerDc(dc))
                  .entityRefFor(LWWHelloWorld.EntityType, entityId)
                  .ask(LWWHelloWorld.SetGreeting(s"hello $greetingNo from ${dc.id}", _), askTimeout)
                  .asScala
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
                    val entityRef = ClusterSharding
                      .get(testKit.system)
                      .entityRefFor(LWWHelloWorld.EntityType, entityId)

                    entityRef
                      .ask(LWWHelloWorld.Get.apply, askTimeout)
                      .asScala
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
    system.log.info("Consumer filter test starting")
    val entityId = nextPid(LWWHelloWorld.EntityType.name).entityId

    ConsumerFilter(systemPerDc(EdgeReplicaC)).ref ! UpdateFilter(
      LWWHelloWorld.EntityType.name,
      List(ConsumerFilter.excludeAll, IncludeTags(Set("tag-C"))))
    ConsumerFilter(systemPerDc(EdgeReplicaD)).ref ! UpdateFilter(
      LWWHelloWorld.EntityType.name,
      List(ConsumerFilter.excludeAll, IncludeTags(Set("tag-D"))))

    // let the filter propagate to producer
    Thread.sleep(1000)
    system.log.info("Continuing after setting IncludeTags")

    ClusterSharding
      .get(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetTag("tag-C", _), askTimeout)
      .asScala
      .futureValue

    ClusterSharding
      .get(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetGreeting("Hello C", _), askTimeout)
      .asScala
      .futureValue

    eventually {
      ClusterSharding
        .get(systemPerDc(EdgeReplicaC))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.Get(_), askTimeout)
        .asScala
        .futureValue shouldBe "Hello C"
    }

    // but not updated in D
    ClusterSharding
      .get(systemPerDc(EdgeReplicaD))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.Get(_), askTimeout)
      .asScala
      .futureValue shouldBe "Hello world"

    system.log.info("Verified filter worked, changing tag on entity")

    // change tag
    ClusterSharding
      .get(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetTag("tag-D", _), askTimeout)
      .asScala
      .futureValue

    // previous greeting should be replicated
    eventually {
      ClusterSharding
        .get(systemPerDc(EdgeReplicaD))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask(LWWHelloWorld.Get(_), askTimeout)
        .asScala
        .futureValue shouldBe "Hello C"
    }

    ClusterSharding
      .get(systemPerDc(CloudReplicaA))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.SetGreeting("Hello D", _), askTimeout)
      .asScala
      .futureValue
    eventually {
      ClusterSharding
        .get(systemPerDc(EdgeReplicaD))
        .entityRefFor(LWWHelloWorld.EntityType, entityId)
        .ask[String](LWWHelloWorld.Get(_), askTimeout)
        .asScala
        .futureValue shouldBe "Hello D"
    }

    // but not updated in C
    ClusterSharding
      .get(systemPerDc(EdgeReplicaC))
      .entityRefFor(LWWHelloWorld.EntityType, entityId)
      .ask(LWWHelloWorld.Get(_), askTimeout)
      .asScala
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
