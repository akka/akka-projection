/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.DurationInt

class ReplicationSettingsSpec extends AnyWordSpec with Matchers {

  trait MyCommand

  "The ReplicationSettings" should {
    "Parse from config" in {
      val system = ActorSystem[Unit](
        Behaviors.empty,
        "parse-test",
        ConfigFactory.parseString("""
         my-replicated-entity {
           entity-event-replication-timeout = 10s
           self-replica-id = dca
           replicas: [
             {
               replica-id: "dca"
               number-of-consumers: 4
               grpc.client: {
                 host = "dca.example.com"
                 port = 8443
               }
             },
             {
               replica-id: "dcb"
               number-of-consumers: 4
               grpc.client: {
                 host = "dcb.example.com"
                 port = 8444
               }
             },
             {
               replica-id: "dcc"
               number-of-consumers: 4
               grpc.client: {
                 host = "dcc.example.com"
                 port = 8445
               }
             }
           ]
         }
         """))

      try {
        val settings = ReplicationSettings[MyCommand]("my-replicated-entity", system)
        settings.streamId should ===("my-replicated-entity")
        settings.entityEventReplicationTimeout should ===(10.seconds)
        settings.selfReplicaId.id should ===("dca")
        settings.otherReplicas.map(_.replicaId.id) should ===(Set("dcb", "dcc"))
        settings.otherReplicas.forall(_.numberOfConsumers === 4) should ===(true)

        val replicaB = settings.otherReplicas.find(_.replicaId.id == "dcb").get
        replicaB.grpcClientSettings.defaultPort should ===(8444)
        replicaB.grpcClientSettings.serviceName should ===("dcb.example.com")

      } finally {
        ActorTestKit.shutdown(system)
      }
    }
  }

}
