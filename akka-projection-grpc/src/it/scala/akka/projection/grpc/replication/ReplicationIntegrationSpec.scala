/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
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
import akka.projection.grpc.TestData
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.scaladsl.ReplicatedEventSourcingOverGrpc
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration._

object ReplicationIntegrationSpec {

  private def config: Config =
    ConfigFactory.parseString("""
       akka.actor.provider = cluster
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
        akka.remote.artery.canonical.host = "127.0.0.1"
        akka.remote.artery.canonical.port = 0
        akka.actor.testkit.typed.filter-leeway = 10s
      """)

  sealed trait Command
  case class Get(replyTo: ActorRef[String]) extends Command
  case class SetGreeting(newGreeting: String, replyTo: ActorRef[Done]) extends Command

  sealed trait Event
  case class GreetingChanged(greeting: String, timestamp: LwwTime) extends Event

  object State {
    val initial = State("Hello world", LwwTime(Long.MinValue, ReplicaId("")))
  }
  case class State(greeting: String, timestamp: LwwTime)

  def LWWHelloWorld(replicationContext: ReplicationContext) =
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

class ReplicationIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit
    with AnyWordSpecLike
    with TestDbLifecycle
    with TestData
    with BeforeAndAfterAll
    with LogCapturing {
  import ReplicationIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq(
    akka.actor.ActorSystem("SystemA", ReplicationIntegrationSpec.config.withFallback(testContainerConf.config)).toTyped,
    akka.actor.ActorSystem("SystemB", ReplicationIntegrationSpec.config.withFallback(testContainerConf.config)).toTyped,
    akka.actor.ActorSystem("SystemC", ReplicationIntegrationSpec.config.withFallback(testContainerConf.config)).toTyped)

  private val testKits = systems.map(ActorTestKit(_))

  private val grpcPorts = SocketUtil.temporaryServerAddresses(3, "127.0.0.1").map(_.getPort)

  private val allReplicaIdsAndPorts = Seq(ReplicaId("DCA"), ReplicaId("DCB"), ReplicaId("DCC")).zip(grpcPorts)
  private val allReplicas = allReplicaIdsAndPorts.map {
    case (id, port) =>
      Replica(
        id,
        2,
        // FIXME can we avoid having to list/match stream id here?
        GrpcQuerySettings("hello-world"),
        GrpcClientSettings.connectToServiceAt("127.0.0.1", port))
  }

  def startReplica(system: ActorSystem[_], selfReplicaId: ReplicaId): ReplicatedEventSourcingOverGrpc[Command] = {
    val settings = ReplicationSettings[Command](
      "hello-world",
      selfReplicaId,
      EventProducerSettings(system),
      allReplicas.filterNot(_.replicaId == selfReplicaId).toSet)
    ReplicatedEventSourcingOverGrpc.grpcReplication(settings)(ReplicationIntegrationSpec.LWWHelloWorld)(system)
  }

  "Replication over gRPC" should {
    "form three one node clusters" in {
      testKits.foreach { testKit =>
        val cluster = Cluster(testKit.system)
        cluster.manager ! Join(cluster.selfMember.address)
        testKit.createTestProbe().awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
        }
      }
    }
    "start three replicas" in {
      Future
        .sequence(allReplicas.zipWithIndex.map {
          case (replica, index) =>
            val system = systems(index)
            logger
              .info(
                "Starting replica [{}]/system [{}] on port [{}]",
                replica.replicaId,
                system.name,
                replica.grpcClientSettings.servicePortName)
            val started = startReplica(system, replica.replicaId)
            val grpcPort = grpcPorts(index)

            // start producer server
            Http(system)
              .newServerAt("127.0.0.1", grpcPort)
              .bind(started.service)
              .map(_.addToCoordinatedShutdown(3.seconds)(system))(system.executionContext)

        })
        .futureValue
      logger.info("All three replication/producer services bound")
    }

  }

  protected override def afterAll(): Unit = {
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}
