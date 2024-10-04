/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
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
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.r2dbc.R2dbcSettings
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.ReplicatedBehaviors
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcReplication
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

object ControlledReplicationIntegrationSpec {

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.actor {
         serialization-bindings {
           "${classOf[ControlledReplicationIntegrationSpec].getName}$$TestEntity$$Event" = jackson-json
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

  private val DCA = ReplicaId("DCA")
  private val DCB = ReplicaId("DCB")
  private val DCC = ReplicaId("DCC")

  object TestEntity {

    val EntityType: EntityTypeKey[Command] = EntityTypeKey[Command]("items")

    sealed trait Command
    final case class Get(replyTo: ActorRef[State]) extends Command
    final case class UpdateItem(key: String, value: Int, replyTo: ActorRef[Done]) extends Command
    final case class SetScope(scope: Set[String], replyTo: ActorRef[Done]) extends Command

    sealed trait Event
    final case class ItemUpdated(key: String, value: Int) extends Event
    final case class ScopeChanged(scope: Set[String]) extends Event

    object State {
      val initial =
        State(Map.empty, Set.empty)
    }

    case class State(items: Map[String, Int], scope: Set[String]) {
      def updateItem(key: String, value: Int): State =
        copy(items = items.updated(key, value))

      def updateScope(scope: Set[String]): State =
        copy(scope = scope)
    }

    def apply(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]) =
      replicatedBehaviors.setup { replicationContext =>
        EventSourcedBehavior[Command, Event, State](
          replicationContext.persistenceId,
          State.initial, {
            case (state, Get(replyTo)) =>
              replyTo ! state
              Effect.none
            case (_, UpdateItem(key, value, replyTo)) =>
              Effect
                .persist(ItemUpdated(key, value))
                .thenRun((_: State) => replyTo ! Done)
            case (_, SetScope(scope, replyTo)) =>
              Effect
                .persist(ScopeChanged(scope))
                .thenRun((_: State) => replyTo ! Done)
          }, {
            case (currentState, ItemUpdated(key, value)) =>
              currentState.updateItem(key, value)
            case (currentState, ScopeChanged(scope)) =>
              currentState.updateScope(scope)
          })
          .withTaggerForState {
            case (state, _) => state.scope
          }
      }
  }
}

class ControlledReplicationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ControlledReplicationIntegrationSpecA",
          ControlledReplicationIntegrationSpec
            .config(ControlledReplicationIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import ControlledReplicationIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ControlledReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private lazy val r2dbcSettings = R2dbcSettings(system.settings.config.getConfig("akka.persistence.r2dbc"))

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ControlledReplicationIntegrationSpecB",
        ControlledReplicationIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ControlledReplicationIntegrationSpecC",
        ControlledReplicationIntegrationSpec.config(DCC).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB, DCC).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(id, 1, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }.toSet

  private val testKitsPerDc = Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)), DCC -> ActorTestKit(systems(2)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1), DCC -> systems(2))

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[TestEntity.Command] = {
    val settings = ReplicationSettings[TestEntity.Command](
      TestEntity.EntityType.name,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      allReplicas,
      10.seconds,
      8,
      R2dbcReplication())

    val consumerFilter = Seq(ConsumerFilter.excludeAll, ConsumerFilter.IncludeTags(Set(settings.selfReplicaId.id)))
    val settingsWIthFilter = settings.withInitialConsumerFilter(consumerFilter)
    Replication.grpcReplication(settingsWIthFilter)(ControlledReplicationIntegrationSpec.TestEntity.apply)(
      replicaSystem)
  }

  "Controlled Replication over gRPC" should {
    "form three one node clusters" in {
      testKitsPerDc.values.foreach { testKit =>
        val cluster = Cluster(testKit.system)
        cluster.manager ! Join(cluster.selfMember.address)
        testKit.createTestProbe().awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
        }
      }
    }

    "start three replicas" in {
      val replicasStarted = Future.sequence(allReplicas.zipWithIndex.map {
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
      logger.info("All three replication/producer services bound")
    }

    "control replication by ConsumerFilter tagging" in {
      val entityId = "one"
      val entityRefA = ClusterSharding(systemPerDc(DCA)).entityRefFor(TestEntity.EntityType, entityId)
      val entityRefB = ClusterSharding(systemPerDc(DCB)).entityRefFor(TestEntity.EntityType, entityId)
      val entityRefC = ClusterSharding(systemPerDc(DCC)).entityRefFor(TestEntity.EntityType, entityId)

      // update in A
      entityRefA.ask(TestEntity.UpdateItem("A", 0, _)).futureValue
      entityRefA.ask(TestEntity.UpdateItem("A", 1, _)).futureValue

      // replicate to B
      entityRefA.ask(TestEntity.SetScope(Set(DCB.id), _)).futureValue
      eventually {
        entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1), Set(DCB.id))
      }
      entityRefC.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State.initial

      // update in B
      entityRefB.ask(TestEntity.UpdateItem("B", 2, _)).futureValue
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCB.id))
      logger.info("Replicate from B to A")
      // replicate to A
      entityRefB.ask(TestEntity.SetScope(Set(DCA.id), _)).futureValue
      eventually {
        entityRefA.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))
      }

      // replicate to C
      entityRefA.ask(TestEntity.SetScope(Set(DCC.id), _)).futureValue
      eventually {
        entityRefC.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCC.id))
      }
      // update in C
      entityRefC.ask(TestEntity.UpdateItem("C", 3, _)).futureValue

      // replicate to A
      entityRefC.ask(TestEntity.SetScope(Set(DCA.id), _)).futureValue
      eventually {
        entityRefA.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(
          Map("A" -> 1, "B" -> 2, "C" -> 3),
          Set(DCA.id))
      }

    }

    "not replicate filtered events after replay trigger" in {
      val entityId = "two"
      val entityRefA = ClusterSharding(systemPerDc(DCA)).entityRefFor(TestEntity.EntityType, entityId)
      val entityRefB = ClusterSharding(systemPerDc(DCB)).entityRefFor(TestEntity.EntityType, entityId)
      val entityRefC = ClusterSharding(systemPerDc(DCC)).entityRefFor(TestEntity.EntityType, entityId)

      // update in A
      entityRefA.ask(TestEntity.UpdateItem("A", 0, _)).futureValue
      entityRefA.ask(TestEntity.UpdateItem("A", 1, _)).futureValue

      // replicate to B
      logger.info("Replicate from A to B")
      entityRefA.ask(TestEntity.SetScope(Set(DCB.id), _)).futureValue
      eventually {
        entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1), Set(DCB.id))
      }

      // update in B
      entityRefB.ask(TestEntity.UpdateItem("B", 2, _)).futureValue
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCB.id))
      logger.info("Replicate from B to A")
      // replicate to A
      entityRefB.ask(TestEntity.SetScope(Set(DCA.id), _)).futureValue
      eventually {
        entityRefA.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))
      }

      // replicate to C
      // This will emit event from C to B for event that was scoped for B originally (those between A and B).
      // That will trigger replay request from B to C. Filters are normally not used in replay because
      // the changed filter is typically what is triggering the replay, but filters should still be applied
      // after the event that triggered the replay.
      logger.info("Replicate from A to C")
      entityRefA.ask(TestEntity.SetScope(Set(DCC.id), _)).futureValue
      eventually {
        entityRefC.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCC.id))
      }

      // update in C
      entityRefC.ask(TestEntity.UpdateItem("C", 3, _)).futureValue
      // latest should not be replicated to B
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))
      Thread.sleep(r2dbcSettings.querySettings.backtrackingBehindCurrentTime.toMillis + 200)
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))

      // another update in C
      entityRefC.ask(TestEntity.UpdateItem("C", 4, _)).futureValue
      // latest should still not be replicated to B
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))
      Thread.sleep(r2dbcSettings.querySettings.backtrackingBehindCurrentTime.toMillis + 200)
      entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(Map("A" -> 1, "B" -> 2), Set(DCA.id))

      // replicate to B
      logger.info("Replicate from C to B")
      entityRefC.ask(TestEntity.SetScope(Set(DCB.id), _)).futureValue
      eventually {
        entityRefB.ask(TestEntity.Get.apply).futureValue shouldBe TestEntity.State(
          Map("A" -> 1, "B" -> 2, "C" -> 4),
          Set(DCB.id))
      }

    }

  }

  protected override def afterAll(): Unit = {
    logger.info("Shutting down all three DCs")
    systems.foreach(_.terminate()) // speed up termination by terminating all at the once
    // and then make sure they are completely shutdown
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}
