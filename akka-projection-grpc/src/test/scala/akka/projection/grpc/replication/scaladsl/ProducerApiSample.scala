/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource

object ProducerApiSample {

  trait MyCommand
  object MyReplicatedBehavior {
    def apply(r: ReplicatedBehaviors[MyCommand, String, String]): Behavior[MyCommand] = ???
  }

  def otherReplication: Replication[Unit] = ???
  def multiEventProducers(settings: ReplicationSettings[MyCommand], host: String, port: Int)(
      implicit system: ActorSystem[_]): Unit = {
    // #multi-service
    val replication: Replication[MyCommand] =
      Replication.grpcReplication(settings)(MyReplicatedBehavior.apply)

    val allSources: Set[EventProducerSource] = {
      Set(
        replication.eventProducerService,
        // producers from other replicated entities or gRPC projections
        otherReplication.eventProducerService)
    }
    val route = EventProducer.grpcServiceHandler(allSources)

    val handler = ServiceHandler.concatOrNotFound(route)
    // #multi-service

    val _ = Http(system).newServerAt(host, port).bind(handler)
  }

}
