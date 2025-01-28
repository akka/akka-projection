/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.Done
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.javadsl.ClusterSharding
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.javadsl.Http
import akka.http.javadsl.ServerBinding
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.LwwTime
import akka.persistence.typed.javadsl.CommandHandler
import akka.persistence.typed.javadsl.EventHandler
import akka.persistence.typed.javadsl.EventSourcedBehavior
import akka.persistence.typed.javadsl.ReplicationContext
import akka.projection.grpc.TestContainerConf
import akka.projection.grpc.TestDbLifecycle
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.replication.javadsl.Replica
import akka.projection.grpc.replication.javadsl.ReplicatedBehaviors
import akka.projection.grpc.replication.javadsl.Replication
import akka.projection.grpc.replication.javadsl.ReplicationSettings
import akka.projection.r2dbc.javadsl.R2dbcReplication
import akka.serialization.jackson.CborSerializable
import akka.testkit.SocketUtil
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.time.Duration

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

import java.util

object ReplicationJavaDSLIntegrationSpec {

  def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
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

  object LWWHelloWorld {

    val EntityType: EntityTypeKey[Command] = EntityTypeKey.create(classOf[Command], "hello-world")

    sealed trait Command

    case class Get(replyTo: ActorRef[String]) extends Command

    case class SetGreeting(newGreeting: String, replyTo: ActorRef[Done]) extends Command

    case class SetTag(tag: String, replyTo: ActorRef[Done]) extends Command

    sealed trait Event extends CborSerializable

    case class GreetingChanged(greeting: String, timestamp: LwwTime) extends Event

    final case class TagChanged(tag: String, timestamp: LwwTime) extends Event

    object State {
      val initial = State("Hello world", LwwTime(Long.MinValue, ReplicaId.empty), "")
    }

    case class State(greeting: String, timestamp: LwwTime, tag: String)

    def create(replicatedBehaviors: ReplicatedBehaviors[Command, Event, State]) = {
      Behaviors.setup[Command](context =>
        replicatedBehaviors.setup { replicationContext => new LWWHelloWorldBehavior(context, replicationContext) })
    }

    class LWWHelloWorldBehavior(context: ActorContext[Command], replicationContext: ReplicationContext)
        extends EventSourcedBehavior[Command, Event, State](replicationContext.persistenceId) {
      protected def emptyState: State = State.initial

      protected def commandHandler(): CommandHandler[Command, Event, State] =
        newCommandHandlerBuilder()
          .forAnyState()
          .onCommand(classOf[Get], { (state: State, command: Get) =>
            command.replyTo.tell(state.greeting)
            Effect.none()
          })
          .onCommand(
            classOf[SetGreeting], { (state: State, command: SetGreeting) =>
              context.getLog.info("Request to change greeting to {}", command.newGreeting)
              Effect
                .persist(
                  GreetingChanged(
                    greeting = command.newGreeting,
                    timestamp =
                      state.timestamp.increase(replicationContext.currentTimeMillis(), replicationContext.replicaId)))
                .thenReply(command.replyTo, _ => Done)
            })
          .onCommand(
            classOf[SetTag], { (state: State, command: SetTag) =>
              context.getLog.info("Request to change tag to {}", command.tag)
              Effect
                .persist(
                  TagChanged(
                    tag = command.tag,
                    timestamp =
                      state.timestamp.increase(replicationContext.currentTimeMillis(), replicationContext.replicaId)))
                .thenReply(command.replyTo, _ => Done)
            })
          .build()

      protected def eventHandler(): EventHandler[State, Event] =
        newEventHandlerBuilder()
          .forAnyState()
          .onEvent(
            classOf[GreetingChanged], { (currentState: State, event: GreetingChanged) =>
              if (event.timestamp.isAfter(currentState.timestamp)) {
                context.getLog.info("Changing greeting to {}", event.greeting)
                State(event.greeting, event.timestamp, currentState.tag)
              } else {
                context.getLog
                  .info(
                    "Ignoring greeting change to {} since state was changed after {}",
                    event.greeting,
                    currentState.timestamp)
                currentState
              }
            })
          .onEvent(
            classOf[TagChanged], { (currentState: State, event: TagChanged) =>
              if (event.timestamp.isAfter(currentState.timestamp)) {
                context.getLog.info("Changing tag to {}", event.tag)
                State(currentState.greeting, event.timestamp, event.tag)
              } else {
                context.getLog
                  .info("Ignoring tag change to {} since state was changed after {}", event.tag, currentState.timestamp)
                currentState
              }

            })
          .build()

      override def tagsFor(state: State, event: Event): util.Set[String] =
        if (state.tag.isEmpty) util.Set.of()
        else util.Set.of(state.tag)
    }

  }
}

// A shorter version of ReplicationIntegrationSpec covering the Java DSL for bootstrapping
class ReplicationJavaDSLIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationJavaDSLIntegrationSpecA",
          ReplicationJavaDSLIntegrationSpec
            .config(ReplicationJavaDSLIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import ReplicationJavaDSLIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationJavaDSLIntegrationSpecB",
        ReplicationJavaDSLIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped,
    akka.actor
      .ActorSystem(
        "ReplicationJavaDSLIntegrationSpecC",
        ReplicationJavaDSLIntegrationSpec.config(DCC).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB, DCC).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica.create(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }

  private val testKitsPerDc = Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)), DCC -> ActorTestKit(systems(2)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1), DCC -> systems(2))
  private var replicatedEventSourcingOverGrpcPerDc: Map[ReplicaId, Replication[LWWHelloWorld.Command]] = Map.empty
  private val entityIds = Set("one", "two", "three")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[LWWHelloWorld.Command] = {
    val settings = ReplicationSettings
      .create(
        classOf[LWWHelloWorld.Command],
        "hello-world-java",
        selfReplicaId,
        EventProducerSettings.create(replicaSystem),
        allReplicas.toSet.asJava: java.util.Set[Replica],
        Duration.ofSeconds(10),
        8,
        R2dbcReplication.create(system))
      .withEdgeReplication(true)
    Replication.grpcReplication(settings, LWWHelloWorld.create _, replicaSystem)
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
          Http
            .get(system)
            .newServerAt("127.0.0.1", grpcPort)
            .bind(started.createSingleServiceHandler())
            .asScala
            .map { (binding: ServerBinding) =>
              binding.addToCoordinatedShutdown(Duration.ofSeconds(3), system)
              replica.replicaId -> started
            }

      })

      replicatedEventSourcingOverGrpcPerDc = replicasStarted.futureValue.toMap
      logger.info("All three replication/producer services bound")
    }

    "replicate writes from one dc to the other two" in {
      val entityTypeKey = replicatedEventSourcingOverGrpcPerDc.values.head.entityTypeKey
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          Future
            .sequence(entityIds.map { entityId =>
              logger.info("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
              ClusterSharding
                .get(systemPerDc(dc))
                .entityRefFor(entityTypeKey, entityId)
                .ask(LWWHelloWorld.SetGreeting(s"hello 1 from ${dc.id}", _), Duration.ofSeconds(3))
                .asScala
            })
            .futureValue

          testKitsPerDc.values.foreach { testKit =>
            withClue(s"on ${system.name}") {
              val probe = testKit.createTestProbe()

              entityIds.foreach { entityId =>
                withClue(s"for entity id $entityId") {
                  val entityRef = ClusterSharding
                    .get(testKit.system)
                    .entityRefFor(entityTypeKey, entityId)

                  probe.awaitAssert({
                    entityRef
                      .ask(LWWHelloWorld.Get.apply, Duration.ofSeconds(10))
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
