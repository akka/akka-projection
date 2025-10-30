/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.Http
import akka.projection.grpc.consumer.scaladsl.EventProducerPushDestination
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
        replication.eventProducerSource,
        // producers from other replicated entities or gRPC projections
        otherReplication.eventProducerSource)
    }
    val route = EventProducer.grpcServiceHandler(allSources)

    val handler = ServiceHandler.concatOrNotFound(route)
    // #multi-service

    // RES push to combine with multiple other services
    val _ = EventProducerPushDestination.grpcServiceHandler(replication.eventProducerPushDestination.toSet)

    val _ = Http().newServerAt(host, port).bind(handler)
  }

}
