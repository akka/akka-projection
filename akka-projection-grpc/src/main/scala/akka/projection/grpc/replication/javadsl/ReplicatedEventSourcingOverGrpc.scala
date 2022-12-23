/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.persistence.typed.javadsl.EventSourcedBehavior
import akka.persistence.typed.javadsl.ReplicationContext
import akka.projection.grpc.producer.javadsl.EventProducerSource
import akka.projection.grpc.replication.ReplicationSettings

import java.util.concurrent.CompletionStage
import java.util.function.{ Function => JFunction }

/**
 * Created using [[ReplicatedEventSourcingOverGrpc#grpcRepliation]], which starts sharding with the entity and
 * replication stream consumers but not the replication endpoint needed to publish events to other replication places.
 *
 * @param eventProducerService If combining multiple entity types replicated, or combining with direct usage of
 *                             Akka Projection gRPC you will have to use the EventProducerService of each of them
 *                             in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
 * @param createSingleServiceHandler If only replicating one Replicated Event Sourced Entity and not using
 *                                   Akka Projection gRPC this endpoint factory can be used to get a partial function
 *                                   that can be served/bound with an Akka HTTP server
 * @param entityTypeKey Entity type key for looking up the
 * @param entityRefFactory Shortcut for creating EntityRefs for the sharded Replicated Event Sourced entities for
 *                         sending commands.
 * @tparam Command The type of commands the Replicated Event Sourced Entity accepts
 */
@ApiMayChange
final class ReplicatedEventSourcingOverGrpc[Command](
    val eventProducerService: EventProducerSource,
    val createSingleServiceHandler: java.util.function.Supplier[
      PartialFunction[HttpRequest, CompletionStage[HttpResponse]]],
    val entityTypeKey: EntityTypeKey[Command],
    val entityRefFactory: JFunction[String, EntityRef[Command]])

@ApiMayChange
object ReplicatedEventSourcingOverGrpc {

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: JFunction[ReplicationContext, EventSourcedBehavior[Command, Event, State]],
      system: ActorSystem[_]): ReplicatedEventSourcingOverGrpc[Command] =
    ???

}
