/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.LwwTime
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.scaladsl.ReplicatedEventSourcingOverGrpc
import akka.projection.r2dbc.R2dbcProjectionSettings
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

object ReplicatedEventSourcingOverGrpcIntegrationSpec {

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.actor {
         serialization-bindings {
           "akka.projection.grpc.replication.ReplicatedEventSourcingOverGrpcIntegrationSpec$$LWWHelloWorld$$Event" = jackson-json
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
        akka.actor.testkit.typed.filter-leeway = 10s
      """)

  private val DCA = ReplicaId("DCA")
  private val DCB = ReplicaId("DCB")
  private val DCC = ReplicaId("DCC")

  object LWWHelloWorld {

    sealed trait Command

    case class Get(replyTo: ActorRef[String]) extends Command

    case class SetGreeting(newGreeting: String, replyTo: ActorRef[Done]) extends Command

    sealed trait Event

    case class GreetingChanged(greeting: String, timestamp: LwwTime) extends Event

    object State {
      val initial = State("Hello world", LwwTime(Long.MinValue, ReplicaId("")))
    }

    case class State(greeting: String, timestamp: LwwTime)

    // FIXME needing to return/pass an EventSourcedBehavior means it is impossible to compose for logging/setup etc
    def apply(replicationContext: ReplicationContext) =
      EventSourcedBehavior[Command, Event, State](
        replicationContext.persistenceId,
        State.initial, {
          case (State(greeting, _), Get(replyTo)) =>
            replyTo ! greeting
            Effect.none
          case (state, SetGreeting(greeting, replyTo)) =>
            Effect
              .persist(
                GreetingChanged(
                  greeting,
                  state.timestamp.increase(replicationContext.currentTimeMillis(), replicationContext.replicaId)))
              .thenRun((_: State) => replyTo ! Done)
        }, {
          case (currentState, GreetingChanged(newGreeting, newTimestamp)) =>
            if (newTimestamp.isAfter(currentState.timestamp))
              State(newGreeting, newTimestamp)
            else currentState
        })
  }
}

class ReplicatedEventSourcingOverGrpcIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationIntegrationSpecA",
          ReplicatedEventSourcingOverGrpcIntegrationSpec
            .config(ReplicatedEventSourcingOverGrpcIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll {
  import ReplicatedEventSourcingOverGrpcIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicatedEventSourcingOverGrpcIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationIntegrationSpecB",
        ReplicatedEventSourcingOverGrpcIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationIntegrationSpecC",
        ReplicatedEventSourcingOverGrpcIntegrationSpec.config(DCC).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB, DCC).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(
        id,
        2,
        // FIXME can we avoid having to list/match stream id here?
        GrpcQuerySettings("hello-world"),
        GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }

  private val testKitsPerDc = Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)), DCC -> ActorTestKit(systems(2)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1), DCC -> systems(2))
  private var replicatedEventSourcingOverGrpcPerDc
      : Map[ReplicaId, ReplicatedEventSourcingOverGrpc[LWWHelloWorld.Command]] = Map.empty

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

  def startReplica(
      replicaSystem: ActorSystem[_],
      selfReplicaId: ReplicaId): ReplicatedEventSourcingOverGrpc[LWWHelloWorld.Command] = {
    val settings = ReplicationSettings[LWWHelloWorld.Command](
      "hello-world",
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      allReplicas.filterNot(_.replicaId == selfReplicaId).toSet)
    ReplicatedEventSourcingOverGrpc.grpcReplication(settings)(
      ReplicatedEventSourcingOverGrpcIntegrationSpec.LWWHelloWorld.apply)(replicaSystem)
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
          Http(system)
            .newServerAt("127.0.0.1", grpcPort)
            .bind(started.createSingleServiceHandler())
            .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)
            .map(_ => replica.replicaId -> started)
      })

      replicatedEventSourcingOverGrpcPerDc = replicasStarted.futureValue.toMap
      logger.info("All three replication/producer services bound")
    }

    "replicate writes from one dc to the other two" in {
      val entityTypeKey = replicatedEventSourcingOverGrpcPerDc.values.head.entityTypeKey
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          logger.info("Updating greeting from dc [{}]", dc.id)
          ClusterSharding(systemPerDc(dc))
            .entityRefFor(entityTypeKey, "one")
            .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${dc.id}", _))
            .futureValue

          testKitsPerDc.values.foreach { testKit =>
            withClue(s"on ${system.name}") {
              val probe = testKit.createTestProbe()

              val entityRef = ClusterSharding(testKit.system)
                .entityRefFor(entityTypeKey, "one")

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

    "replicate concurrent writes to the other DCs" in {
      val entityTypeKey = replicatedEventSourcingOverGrpcPerDc.values.head.entityTypeKey
      Future
        .sequence(systemPerDc.keys.map { dc =>
          withClue(s"from ${dc.id}") {
            logger.info("Updating greeting from dc [{}]", dc.id)
            ClusterSharding(systemPerDc(dc))
              .entityRefFor(entityTypeKey, "one")
              .ask(LWWHelloWorld.SetGreeting(s"hello 2 from ${dc.id}", _))
          }
        })
        .futureValue // all three updated in roughly parallel

      // All 3 should eventually arrive at the same value
      testKit
        .createTestProbe()
        .awaitAssert(
          {
            testKitsPerDc.values.map { testKit =>
              val entityRef = ClusterSharding(testKit.system)
                .entityRefFor(entityTypeKey, "one")

              entityRef
                .ask(LWWHelloWorld.Get.apply)
                .futureValue
            }.toSet should have size (1)
          },
          20.seconds)

    }
  }

  protected override def afterAll(): Unit = {
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}