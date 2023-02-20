/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.projection.grpc.replication.scaladsl.ReplicationProjectionProvider
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
import akka.projection.grpc.replication.javadsl.{ ReplicationSettings => JReplicationSettings }
import akka.projection.grpc.replication.javadsl.{ ReplicationProjectionProvider => JReplicationProjectionProvider }
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

class ReplicationSettingsSpec extends AnyWordSpec with Matchers {

  trait MyCommand

  "The ReplicationSettings" should {
    "Parse from config" in {
      implicit val system: ActorSystem[Unit] = ActorSystem[Unit](
        Behaviors.empty[Unit],
        "parse-test",
        ConfigFactory.parseString("""
         // #config
         my-replicated-entity {
           # which of the replicas this node belongs to, should be the same
           # across the nodes of each replica Akka cluster.
           self-replica-id = dca
           # Pick it up from an environment variable to re-use the same config
           # without changes across replicas
           self-replica-id = ${?SELF_REPLICA}
           # max number of parallel in-flight (sent over sharding) entity updates
           # per consumer/projection
           parallel-updates = 8
           # Fail the replication stream (and restart with backoff) if completing
           # the write of a replicated event reaching the cluster takes more time
           # than this.
           entity-event-replication-timeout = 10s
           replicas: [
             {
               # Unique identifier of the replica/datacenter, is stored in the events
               # and cannot be changed after events have been persisted.
               replica-id = "dca"
               # Number of replication streams/projections to start to consume events
               # from this replica
               number-of-consumers = 4
               # Akka gRPC client config block for how to reach this replica
               # from the other replicas, note that binding the server/publishing
               # endpoint of each replica is done separately, in code.
               grpc.client {
                 host = "dca.example.com"
                 port = 8443
                 use-tls = true
               }
             },
             {
               replica-id = "dcb"
               number-of-consumers = 4
               # Optional - only run replication stream consumers for events from the
               # remote replica on nodes with this role
               consumers-on-cluster-role = dcb-consumer
               grpc.client {
                 host = "dcb.example.com"
                 port = 8444
               }
             },
             {
               replica-id = "dcc"
               number-of-consumers = 4
               grpc.client {
                 host = "dcc.example.com"
                 port = 8445
               }
             }
           ]
         }
         // #config
         """).resolve())

      try {
        val settings = ReplicationSettings[MyCommand](
          "my-replicated-entity",
          // never actually used, just passed along
          null: ReplicationProjectionProvider)
        settings.streamId should ===("my-replicated-entity")
        settings.entityEventReplicationTimeout should ===(10.seconds)
        settings.selfReplicaId.id should ===("dca")
        settings.otherReplicas.map(_.replicaId.id) should ===(Set("dcb", "dcc"))
        settings.otherReplicas.forall(_.numberOfConsumers === 4) should ===(true)
        settings.parallelUpdates should ===(8)

        val replicaB = settings.otherReplicas.find(_.replicaId.id == "dcb").get
        replicaB.grpcClientSettings.defaultPort should ===(8444)
        replicaB.grpcClientSettings.serviceName should ===("dcb.example.com")
        replicaB.consumersOnClusterRole should ===(Some("dcb-consumer"))

        // And Java DSL
        val javaSettings = JReplicationSettings.create(
          classOf[MyCommand],
          "my-replicated-entity",
          // never actually used, just passed along
          null: JReplicationProjectionProvider,
          system)

        val converted = javaSettings.toScala
        converted.selfReplicaId should ===(settings.selfReplicaId)
        converted.streamId should ===(settings.streamId)

        converted.otherReplicas.foreach { replica =>
          val scalaReplica = settings.otherReplicas.find(_.replicaId == replica.replicaId).get
          replica.consumersOnClusterRole should ===(scalaReplica.consumersOnClusterRole)
          replica.numberOfConsumers should ===(scalaReplica.numberOfConsumers)
          // no equals on GrpcClientSettings
          replica.grpcClientSettings.serviceName === (scalaReplica.grpcClientSettings.serviceName)
          replica.grpcClientSettings.defaultPort === (scalaReplica.grpcClientSettings.defaultPort)
          replica.grpcClientSettings.useTls === (scalaReplica.grpcClientSettings.useTls)
        }

        converted.entityEventReplicationTimeout should ===(settings.entityEventReplicationTimeout)
        converted.entityTypeKey === (settings.entityTypeKey)
        converted.eventProducerInterceptor === (settings.eventProducerInterceptor)
        converted.projectionProvider === (settings.projectionProvider)
        converted.parallelUpdates === (settings.parallelUpdates)

      } finally {
        ActorTestKit.shutdown(system)
      }
    }
  }

}
