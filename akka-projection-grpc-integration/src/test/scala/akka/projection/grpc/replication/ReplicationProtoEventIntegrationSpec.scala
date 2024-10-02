/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.testkit.typed.scaladsl.LogCapturing
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.MemberStatus
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.grpc.GrpcClientSettings
import akka.http.scaladsl.Http
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.crdt.LwwTime
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
import akka.serialization.BaseSerializer
import akka.serialization.ByteBufferSerializer
import akka.serialization.SerializerWithStringManifest
import akka.testkit.SocketUtil
import com.google.protobuf.CodedInputStream
import com.google.protobuf.CodedOutputStream
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import scalapb.GeneratedMessage
import scalapb.GeneratedMessageCompanion

import java.nio.ByteBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

object ReplicationProtoEventIntegrationSpec {

  class ProtobufSerializer(val system: ExtendedActorSystem)
      extends SerializerWithStringManifest
      with BaseSerializer
      with ByteBufferSerializer {

    private val TagChangedManifest = akka.projection.grpc.test.TagChanged.javaDescriptor.getName
    private val GreetingChangedManifest = akka.projection.grpc.test.GreetingChanged.javaDescriptor.getName

    override def manifest(o: AnyRef): String = {
      o match {
        case msg: GeneratedMessage => msg.companion.javaDescriptor.getName
        case _ =>
          throw new IllegalArgumentException(s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]")
      }
    }

    override def toBinary(o: AnyRef): Array[Byte] = {
      o match {
        case msg: GeneratedMessage => msg.toByteArray
        case _ =>
          throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
      }
    }

    override def toBinary(o: AnyRef, buf: ByteBuffer): Unit = {
      o match {
        case msg: GeneratedMessage =>
          val output = CodedOutputStream.newInstance(buf)
          msg.writeTo(output)
          output.flush()
        case _ =>
          throw new IllegalArgumentException(s"Cannot serialize object of type [${o.getClass.getName}]")
      }
    }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
      getCompanion(manifest).parseFrom(bytes).asInstanceOf[AnyRef]
    }

    override def fromBinary(buf: ByteBuffer, manifest: String): AnyRef = {
      getCompanion(manifest).parseFrom(CodedInputStream.newInstance(buf)).asInstanceOf[AnyRef]
    }

    private def getCompanion(manifest: String): GeneratedMessageCompanion[_] = manifest match {
      case GreetingChangedManifest => akka.projection.grpc.test.GreetingChanged.messageCompanion
      case TagChangedManifest      => akka.projection.grpc.test.TagChanged.messageCompanion
      case unknown                 => throw new IllegalArgumentException(s"Unknown manifest $unknown")
    }

  }

  private def config(dc: ReplicaId): Config =
    ConfigFactory.parseString(s"""
       akka.actor.provider = cluster
       akka.actor {
         serializers {
           my-replication-serializer = "akka.projection.grpc.replication.ReplicationProtoEventIntegrationSpec$$ProtobufSerializer"
         }
         serialization-identifiers {
           "akka.projection.grpc.replication.ReplicationProtoEventIntegrationSpec$$ProtobufSerializer" = 1528148901
         }
         serialization-bindings {
           "scalapb.GeneratedMessage" = my-replication-serializer
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

  object LWWHelloWorld {

    val EntityType: EntityTypeKey[Command] = EntityTypeKey[Command]("hello-world")

    sealed trait Command
    final case class Get(replyTo: ActorRef[String]) extends Command
    final case class SetGreeting(newGreeting: String, replyTo: ActorRef[Done]) extends Command
    final case class SetTag(tag: String, replyTo: ActorRef[Done]) extends Command

    // events are defined directly as proto messages
    private implicit class ProtoLwwToScalaLww(protoLwwTime: akka.projection.grpc.test.LwwTime) {
      def toScala: LwwTime = LwwTime(protoLwwTime.timestamp, ReplicaId(protoLwwTime.originReplica))
    }
    private implicit class ScalaLwwToProtoLww(lwwTime: LwwTime) {
      def toProto: akka.projection.grpc.test.LwwTime =
        akka.projection.grpc.test.LwwTime(lwwTime.timestamp, lwwTime.originReplica.id)
    }

    object State {
      val initial =
        State("Hello world", LwwTime(Long.MinValue, ReplicaId("")), "", LwwTime(Long.MinValue, ReplicaId("")))
    }

    case class State(greeting: String, greetingTimestamp: LwwTime, tag: String, tagTimestamp: LwwTime)

    def apply(replicatedBehaviors: ReplicatedBehaviors[Command, GeneratedMessage, State]) =
      replicatedBehaviors.setup { replicationContext =>
        EventSourcedBehavior[Command, GeneratedMessage, State](
          replicationContext.persistenceId,
          State.initial, {
            case (State(greeting, _, _, _), Get(replyTo)) =>
              replyTo ! greeting
              Effect.none
            case (state, SetGreeting(greeting, replyTo)) =>
              Effect
                .persist(
                  akka.projection.grpc.test.GreetingChanged(
                    greeting,
                    Some(state.greetingTimestamp
                      .increase(replicationContext.currentTimeMillis(), replicationContext.replicaId)
                      .toProto)))
                .thenRun((_: State) => replyTo ! Done)
            case (state, SetTag(tag, replyTo)) =>
              Effect
                .persist(
                  akka.projection.grpc.test.TagChanged(
                    tag,
                    Some(state.greetingTimestamp
                      .increase(replicationContext.currentTimeMillis(), replicationContext.replicaId)
                      .toProto)))
                .thenRun((_: State) => replyTo ! Done)
          }, {
            case (currentState, akka.projection.grpc.test.GreetingChanged(newGreeting, newTimestamp, _)) =>
              if (newTimestamp.get.toScala.isAfter(currentState.greetingTimestamp))
                currentState.copy(newGreeting, newTimestamp.get.toScala)
              else currentState
            case (currentState, akka.projection.grpc.test.TagChanged(newTag, newTimestamp, _)) =>
              if (newTimestamp.get.toScala.isAfter(currentState.tagTimestamp))
                currentState.copy(tag = newTag, tagTimestamp = newTimestamp.get.toScala)
              else currentState
            case (_, unknown) =>
              throw new IllegalArgumentException(s"Unknown proto event type ${unknown.getClass}")
          })
          .withTaggerForState {
            case (state, _) => if (state.tag == "") Set.empty else Set(state.tag)
          }
      }
  }
}

class ReplicationProtoEventIntegrationSpec(testContainerConf: TestContainerConf)
    extends ScalaTestWithActorTestKit(
      akka.actor
        .ActorSystem(
          "ReplicationProtoEventIntegrationSpecA",
          ReplicationProtoEventIntegrationSpec
            .config(ReplicationProtoEventIntegrationSpec.DCA)
            .withFallback(testContainerConf.config))
        .toTyped)
    with AnyWordSpecLike
    with TestDbLifecycle
    with BeforeAndAfterAll
    with LogCapturing {
  import ReplicationProtoEventIntegrationSpec._
  implicit val ec: ExecutionContext = system.executionContext

  def this() = this(new TestContainerConf)

  private val logger = LoggerFactory.getLogger(classOf[ReplicationIntegrationSpec])
  override def typedSystem: ActorSystem[_] = testKit.system

  private val systems = Seq[ActorSystem[_]](
    typedSystem,
    akka.actor
      .ActorSystem(
        "ReplicationIntegrationSpecB",
        ReplicationProtoEventIntegrationSpec.config(DCB).withFallback(testContainerConf.config))
      .toTyped)

  private val grpcPorts = SocketUtil.temporaryServerAddresses(systems.size, "127.0.0.1").map(_.getPort)
  private val allDcsAndPorts = Seq(DCA, DCB).zip(grpcPorts)
  private val allReplicas = allDcsAndPorts.map {
    case (id, port) =>
      Replica(id, 2, GrpcClientSettings.connectToServiceAt("127.0.0.1", port).withTls(false))
  }.toSet

  private val testKitsPerDc = Map(DCA -> testKit, DCB -> ActorTestKit(systems(1)))
  private val systemPerDc = Map(DCA -> system, DCB -> systems(1))
  private val entityIds = Set("one", "two", "three")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    systemPerDc.values.foreach(beforeAllDeleteFromTables)
  }

  def startReplica(replicaSystem: ActorSystem[_], selfReplicaId: ReplicaId): Replication[LWWHelloWorld.Command] = {
    val settings = ReplicationSettings[LWWHelloWorld.Command](
      LWWHelloWorld.EntityType.name,
      selfReplicaId,
      EventProducerSettings(replicaSystem),
      allReplicas,
      10.seconds,
      8,
      R2dbcReplication())
    Replication.grpcReplication(settings)(ReplicationProtoEventIntegrationSpec.LWWHelloWorld.apply)(replicaSystem)
  }

  "Replication over gRPC with protobuf events" should {
    "form two one node clusters" in {
      testKitsPerDc.values.foreach { testKit =>
        val cluster = Cluster(testKit.system)
        cluster.manager ! Join(cluster.selfMember.address)
        testKit.createTestProbe().awaitAssert {
          cluster.selfMember.status should ===(MemberStatus.Up)
        }
      }
    }

    "start two replicas" in {
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
      logger.info("Both replication/producer services bound")
    }

    "replicate writes from one dc to the other" in {
      systemPerDc.keys.foreach { dc =>
        withClue(s"from ${dc.id}") {
          Future
            .sequence(entityIds.map { entityId =>
              logger.infoN("Updating greeting for [{}] from dc [{}]", entityId, dc.id)
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
  }

  protected override def afterAll(): Unit = {
    logger.info("Shutting down both DCs")
    systems.foreach(_.terminate()) // speed up termination by terminating all at the once
    // and then make sure they are completely shutdown
    systems.foreach { system =>
      ActorTestKit.shutdown(system)
    }
    super.afterAll()
  }
}
