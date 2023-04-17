/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.replication.internal.ReplicationImpl
import scala.concurrent.Future

import akka.persistence.query.typed.EventEnvelope

/**
 * Created using [[Replication.grpcReplication]], which starts sharding with the entity and
 * replication stream consumers but not the replication endpoint needed to publish events to other replication places.
 *
 * @tparam Command The type of commands the Replicated Event Sourced Entity accepts
 *
 * Not for user extension
 */
@ApiMayChange
@DoNotInherit
trait Replication[Command] {

  /**
   * If combining multiple replicated entity types, or combining with direct usage of
   * Akka Projection gRPC, you will have to use the EventProducerService of each of them
   * in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
   */
  def eventProducerService: EventProducerSource

  /**
   * If only replicating one Replicated Event Sourced Entity and not using
   * Akka Projection gRPC this endpoint factory can be used to get a partial function
   * that can be served/bound with an Akka HTTP/2 server
   */
  def createSingleServiceHandler: () => PartialFunction[HttpRequest, Future[HttpResponse]]

  /**
   * Entity type key for looking up the entities
   */
  def entityTypeKey: EntityTypeKey[Command]

  /**
   * Shortcut for creating EntityRefs for the sharded Replicated Event Sourced entities for
   * sending commands.
   */
  def entityRefFactory: String => EntityRef[Command]
}

@ApiMayChange
object Replication {

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): Replication[Command] =
    grpcReplication[Command, Event, State](settings, (_: EventEnvelope[Event]) => true)(replicatedBehaviorFactory)

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      producerFilter: EventEnvelope[Event] => Boolean)(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): Replication[Command] = {

    val replicatedEntity =
      ReplicatedEntity(
        settings.selfReplicaId,
        settings.configureEntity.apply(Entity(settings.entityTypeKey) { entityContext =>
          val replicationId =
            ReplicationId(entityContext.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
          replicatedBehaviorFactory { factory =>
            ReplicatedEventSourcing.externalReplication(
              replicationId,
              settings.otherReplicas.map(_.replicaId) + settings.selfReplicaId)(factory)
          }
        }))

    ReplicationImpl.grpcReplication[Command, Event, State](settings, producerFilter, replicatedEntity)
  }

}
