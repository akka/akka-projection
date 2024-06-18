/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import java.time.Instant

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
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.projection.grpc.TestContainerConf
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

object ReplicationMigrationIntegrationSpec {

  private def config(dc: ReplicaId): Config = {
    val journalTable = if (dc.id == "") "event_journal" else s"event_journal_${dc.id}"
    val timestampOffsetTable =
      if (dc.id == "") "akka_projection_timestamp_offset_store" else s"akka_projection_timestamp_offset_store_${dc.id}"
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

  private val DCA = ReplicaId("")
  private val DCB = ReplicaId("DCB")
  private val DCC = ReplicaId("DCC")

  object HelloWorld {

    val EntityType: EntityTypeKey[Command] = EntityTypeKey[Command]("hello-world")

    sealed trait Command extends JsonSerializable
    final case class Get(replyTo: ActorRef[String]) extends Command
    final case class SetGreeting(newGreeting: String, replyTo: ActorRef[Done]) extends Command

    sealed trait Event extends JsonSerializable
    final case class GreetingChanged(greeting: String, timestamp: Instant) extends Event

    object State {
      val initial =
        State("Hello world", Instant.EPOCH)
    }

    case class State(greeting: String, timestamp: Instant) extends JsonSerializable

    def replicated(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]): Behavior[Command] =
      replicatedBehaviors.setup { replicationContext =>
        nonReplicated(replicationContext.persistenceId)
      }

    def nonReplicated(persistenceId: PersistenceId): EventSourcedBehavior[Command, Event, State] =
      EventSourcedBehavior[Command, Event, State](
        persistenceId,
        State.initial, {
          case (State(greeting, _), Get(replyTo)) =>
            replyTo ! greeting
            Effect.none
          case (_, SetGreeting(greeting, replyTo)) =>
            Effect
              .persist(GreetingChanged(greeting, Instant.now()))
              .thenRun((_: State) => replyTo ! Done)
        }, {
          case (currentState, GreetingChanged(newGreeting, newTimestamp)) =>
            if (newTimestamp.isAfter(currentState.timestamp))
              currentState.copy(newGreeting, newTimestamp)
            else currentState
        })
  }
}

class ReplicationMigrationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationMigrationIntegrationSpecA",
          ReplicationMigrationIntegrationSpec
            .config(ReplicationMigrationIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import ReplicationMigrationIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicationMigrationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationMigrationIntegrationSpecB",
        ReplicationMigrationIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationMigrationIntegrationSpecC",
        ReplicationMigrationIntegrationSpec.config(DCC).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB, DCC).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }.toSet

  private val testKitsPerDc = Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)), DCC -> ActorTestKit(systems(2)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1), DCC -> systems(2))
  private val entityIds = Set("one", "two", "three")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[HelloWorld.Command] = {
    val settings = ReplicationSettings[HelloWorld.Command](
      HelloWorld.EntityType.name,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      allReplicas,
      10.seconds,
      8,
      R2dbcReplication())
    Replication.grpcReplication(settings)(ReplicationMigrationIntegrationSpec.HelloWorld.replicated)(replicaSystem)
  }

  "Replication over gRPC" should {
    "form one cluster" in {
      val testKit = testKitsPerDc.values.head
      val cluster = Cluster(testKit.system)
      cluster.manager ! Join(cluster.selfMember.address)
      testKit.createTestProbe().awaitAssert {
        cluster.selfMember.status should ===(MemberStatus.Up)
      }
    }

    "persist events with non-replicated EventSourcedBehavior" in {
      val testKit = testKitsPerDc.values.head
      entityIds.foreach { entityId =>
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
      // one has already been started
      testKitsPerDc.values.drop(1).foreach { testKit =>
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
            .infoN(
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

    "recover from non-replicated events" in {
      testKitsPerDc.values.foreach { testKit =>
        withClue(s"on ${testKit.system.name}") {
          val probe = testKit.createTestProbe()

          entityIds.foreach { entityId =>
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

    "replicate writes from one dc to the other two" in {
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          Future
            .sequence(entityIds.map { entityId =>
              logger.infoN("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
              ClusterSharding(systemPerDc(dc))
                .entityRefFor(HelloWorld.EntityType, entityId)
                .ask(HelloWorld.SetGreeting(s"hello 3 from ${dc.id}", _))
            })
            .futureValue

          testKitsPerDc.values.foreach { testKit =>
            withClue(s"on ${testKit.system.name}") {
              val probe = testKit.createTestProbe()

              entityIds.foreach { entityId =>
                withClue(s"for entity id $entityId") {
                  val entityRef = ClusterSharding(testKit.system)
                    .entityRefFor(HelloWorld.EntityType, entityId)

                  probe.awaitAssert({
                    entityRef
                      .ask(HelloWorld.Get.apply)
                      .futureValue should ===(s"hello 3 from ${dc.id}")
                  }, 10.seconds)
                }
              }
            }
          }
        }
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
