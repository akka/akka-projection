/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.replication

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.ReplicatedBehaviors
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.serialization.jackson.JsonSerializable
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object ReplicationWithDisabledEventOriginFilterIntegrationSpec {

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.http.server.preview.enable-http2 = on
       akka.persistence.r2dbc {
          journal.table = "event_journal_${dc.id}"
          query {
            refresh-interval = 500 millis
            # reducing this to have quicker test, triggers backtracking earlier
            backtracking.behind-current-time = 3 seconds
            # smaller buffer size to make event delivery more random
            buffer-size = 10
          }
          # disable pub-sub to make event delivery more random
          journal.publish-events = off
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
  private val DCC = ReplicaId("DCC")
  private val DCD = ReplicaId("DCD")
  private val DCE = ReplicaId("DCE")

  object OrderedEntity {

    val EntityType: EntityTypeKey[Command] = EntityTypeKey[Command]("test-entity")

    sealed trait Command extends JsonSerializable
    final case class Get(replyTo: ActorRef[String]) extends Command
    final case class Add(value: String, replyTo: ActorRef[Done]) extends Command

    sealed trait Event extends JsonSerializable
    final case class Added(value: String) extends Event

    object State {
      val empty = State("")
    }

    case class State(values: String)

    def apply(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]): Behavior[Command] =
      replicatedBehaviors.setup { replicationContext =>
        EventSourcedBehavior[Command, Event, State](
          replicationContext.persistenceId,
          State.empty, {
            case (state, Get(replyTo)) =>
              replyTo ! state.values
              Effect.none
            case (_, Add(value, replyTo)) =>
              Effect
                .persist(Added(value))
                .thenRun((_: State) => replyTo ! Done)
          }, {
            case (currentState, Added(value)) =>
              if (currentState.values == "")
                State(value)
              else
                State(currentState.values + "," + value)
          })
      }
  }
}

class ReplicationWithDisabledEventOriginFilterIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationWithDisabledEventOriginFilterIntegrationSpecA",
          ReplicationWithDisabledEventOriginFilterIntegrationSpec
            .config(ReplicationWithDisabledEventOriginFilterIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing
    with TestData {
  import ReplicationWithDisabledEventOriginFilterIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicationWithDisabledEventOriginFilterIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationWithDisabledEventOriginFilterIntegrationSpecB",
        ReplicationWithDisabledEventOriginFilterIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationWithDisabledEventOriginFilterIntegrationSpecC",
        ReplicationWithDisabledEventOriginFilterIntegrationSpec.config(DCC).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationWithDisabledEventOriginFilterIntegrationSpecD",
        ReplicationWithDisabledEventOriginFilterIntegrationSpec.config(DCD).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationWithDisabledEventOriginFilterIntegrationSpecE",
        ReplicationWithDisabledEventOriginFilterIntegrationSpec.config(DCE).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB, DCC, DCD, DCE).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }.toSet

  private val testKitsPerDc = Map(
    DCA -> testKit,
    DCB -> ActorTestKit(systems(1)),
    DCC -> ActorTestKit(systems(2)),
    DCD -> ActorTestKit(systems(3)),
    DCE -> ActorTestKit(systems(4)))
  private val systemPerDc =
    Map(DCA -> system, DCB -> systems(1), DCC -> systems(2), DCD -> systems(3), DCE -> systems(4))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[OrderedEntity.Command] = {
    val otherReplicas = allReplicas.filterNot(_.replicaId == selfReplicaId)

    val settings = ReplicationSettings[OrderedEntity.Command](
      OrderedEntity.EntityType.name,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      otherReplicas,
      10.seconds,
      8,
      R2dbcReplication())
      .withEventOriginFilterEnabled(false)
    Replication.grpcReplication(settings)(OrderedEntity.apply)(replicaSystem)
  }

  def eventuallyAssertValues(entityId: String, expected: String): Unit = {
    // DCE is started later, so not included in assert
    testKitsPerDc.keysIterator.filterNot(_ == DCE).foreach(dc => eventuallyAssertValues(dc, entityId, expected))
  }

  def eventuallyAssertValues(dc: ReplicaId, entityId: String, expected: String): Unit = {
    val testKit = testKitsPerDc(dc)
    withClue(s"on ${testKit.system.name}") {
      val probe = testKit.createTestProbe()
      withClue(s"for entity id $entityId") {
        val entityRef = ClusterSharding(testKit.system)
          .entityRefFor(OrderedEntity.EntityType, entityId)

        probe.awaitAssert({
          entityRef
            .ask(OrderedEntity.Get.apply)(100.millis)
            .futureValue should ===(expected)
        }, 10.seconds)
      }
    }
  }

  def entityRef(dc: ReplicaId, entityId: String): EntityRef[OrderedEntity.Command] = {
    ClusterSharding(systemPerDc(dc)).entityRefFor(OrderedEntity.EntityType, entityId)
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

    "start replicas, except DCE" in {
      val replicasStarted = Future.sequence(allReplicas.filterNot(_.replicaId == DCE).zipWithIndex.map {
        case (replica, index) =>
          val system = systems(index)
          logger
            .info(
              "Starting replica [{}], system [{}] on port [{}]",
              replica.replicaId,
              system.name,
              replica.grpcClientSettings.defaultPort)
          val started = startReplica(system, replica.replicaId)
          val grpcPort = grpcPorts(index)

          // start producer server
          Http()(system)
            .newServerAt("127.0.0.1", grpcPort)
            .bind(started.createSingleServiceHandler())
            .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
            .map(_ => replica.replicaId -> started)
      })

      replicasStarted.futureValue
      logger.info("replication/producer services bound")
    }

    "replicate with preserved event causality" in {
      val entityId = nextPid(OrderedEntity.EntityType.name).entityId

      entityRef(DCA, entityId)
        .ask(OrderedEntity.Add("1", _))
        .futureValue
      eventuallyAssertValues(entityId, "1")

      entityRef(DCA, entityId)
        .ask(OrderedEntity.Add("2", _))
        .futureValue
      eventuallyAssertValues(entityId, "1,2")

      entityRef(DCB, entityId).ask(OrderedEntity.Add("3", _))
      entityRef(DCB, entityId).ask(OrderedEntity.Add("4", _))
      eventuallyAssertValues(DCC, entityId, "1,2,3,4")

      entityRef(DCC, entityId).ask(OrderedEntity.Add("5", _))
      entityRef(DCC, entityId).ask(OrderedEntity.Add("6", _))
      eventuallyAssertValues(DCD, entityId, "1,2,3,4,5,6")

      entityRef(DCD, entityId).ask(OrderedEntity.Add("7", _))
      entityRef(DCD, entityId).ask(OrderedEntity.Add("8", _))
      entityRef(DCD, entityId).ask(OrderedEntity.Add("9", _))
      // same order for all
      eventuallyAssertValues(entityId, "1,2,3,4,5,6,7,8,9")
    }

    "replicate many events with preserved event causality" in {
      val entityId = nextPid(OrderedEntity.EntityType.name).entityId

      // write in A, wait in B
      (0 until 100).foreach { n =>
        entityRef(DCA, entityId).ask(OrderedEntity.Add(n.toString, _))
      }
      eventuallyAssertValues(DCB, entityId, (0 until 100).mkString(","))

      // write in B, wait in C
      (100 until 200).foreach { n =>
        entityRef(DCB, entityId).ask(OrderedEntity.Add(n.toString, _))
      }
      eventuallyAssertValues(DCC, entityId, (0 until 200).mkString(","))

      // write in C, wait in D
      (200 until 300).foreach { n =>
        entityRef(DCC, entityId).ask(OrderedEntity.Add(n.toString, _))
      }
      eventuallyAssertValues(DCD, entityId, (0 until 300).mkString(","))

      // write in D, wait in A
      (300 until 400).foreach { n =>
        entityRef(DCD, entityId).ask(OrderedEntity.Add(n.toString, _))
      }
      eventuallyAssertValues(DCA, entityId, (0 until 400).mkString(","))

      // same order everywhere
      eventuallyAssertValues(entityId, (0 until 400).mkString(","))

      // then start DCE replica, which should still receive the events in the right order
      startReplica(systems.last, DCE)
      eventuallyAssertValues(DCA, entityId, (0 until 400).mkString(","))
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
