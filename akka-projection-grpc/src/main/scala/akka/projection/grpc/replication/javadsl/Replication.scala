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
import akka.projection.grpc.producer.javadsl.EventProducer
import akka.projection.grpc.producer.javadsl.EventProducerSource
import akka.projection.grpc.producer.javadsl.Transformation
import akka.projection.grpc.replication.ReplicationSettings
import akka.projection.grpc.replication.scaladsl

import java.util.concurrent.CompletionStage
import akka.japi.function.{ Function => JFunction }

/**
 * Created using [[Replication#grpcRepliation]], which starts sharding with the entity and
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
final class Replication[Command] private (
    val eventProducerService: EventProducerSource,
    serviceHandlerF: () => JFunction[HttpRequest, CompletionStage[HttpResponse]],
    val entityTypeKey: EntityTypeKey[Command],
    val entityRefFactory: JFunction[String, EntityRef[Command]]) {
  def createSingleServiceHandler(): JFunction[HttpRequest, CompletionStage[HttpResponse]] =
    serviceHandlerF()
}

@ApiMayChange
object Replication {

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      replicatedBehaviorFactory: JFunction[ReplicationContext, EventSourcedBehavior[Command, Event, State]],
      system: ActorSystem[_]): Replication[Command] = {
    val scalaRESOG =
      scaladsl.Replication
        .grpcReplication(settings) {
          case ctx: ReplicationContext =>
            replicatedBehaviorFactory.apply(ctx).createEventSourcedBehavior()
          case other =>
            // Should never happen
            throw new IllegalArgumentException(s"Expected javadsl ReplicationContext but got ${other.getClass}")
        }(system)
    val jEventProducerSource = new EventProducerSource(
      scalaRESOG.eventProducerService.entityType,
      scalaRESOG.eventProducerService.streamId,
      Transformation.fromScala(scalaRESOG.eventProducerService.transformation),
      scalaRESOG.eventProducerService.settings)
    new Replication[Command](
      jEventProducerSource,
      () => EventProducer.grpcServiceHandler(system, jEventProducerSource),
      scalaRESOG.entityTypeKey.asJava,
      (entityId: String) => scalaRESOG.entityRefFactory.apply(entityId).asJava)
  }

}
