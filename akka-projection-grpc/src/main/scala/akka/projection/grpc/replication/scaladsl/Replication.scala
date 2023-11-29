/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import scala.concurrent.Future

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
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.replication.internal.ReplicationImpl

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

    ReplicationImpl.grpcReplication[Command, Event, State](settings, replicatedEntity)
  }

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Filter events matching the `producerFilter` predicate, for example based on tags.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  @deprecated("Define producerFilter via settings.withProducerFilter", "1.5.1")
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      producerFilter: EventEnvelope[Event] => Boolean)(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): Replication[Command] = {
    grpcReplication(settings.withProducerFilter(producerFilter))(replicatedBehaviorFactory)
  }

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Filter events matching the topic expression according to MQTT specification, including wildcards.
   * The topic of an event is defined by a tag with certain prefix, see `topic-tag-prefix` configuration.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  @deprecated("Define topicExpression via settings.withProducerFilterTopicExpression", "1.5.1")
  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command], topicExpression: String)(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): Replication[Command] = {
    grpcReplication(settings.withProducerFilterTopicExpression(topicExpression))(replicatedBehaviorFactory)
  }

  /**
   * Called to bootstrap the entity on each edge cluster node. In edge mode all connections for replication
   * comes from edge node to cloud. Cloud service needs to be configured with edge replication enabled through
   * `ReplicationSettings.withEdgeReplication(true)`.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  // FIXME factory method name
  // FIXME filtering is important here, to control what gets replicated to the edge
  // FIXME a separate settings class because it differs enough?
  // FIXME maybe not grpcClientSettings directly?
  // FIXME replica id of the edge node should not be in the otherReplicas of cloud nodes, is documenting that enough?
  // FIXME replica id should still be unique, is documenting that enough (tricky to do any checks)?
  def grpcEdgeReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): EdgeReplication[Command] = {
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

    ReplicationImpl.grpcEdgeReplication(settings, replicatedEntity)
  }

  trait EdgeReplication[Command] {

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
}
