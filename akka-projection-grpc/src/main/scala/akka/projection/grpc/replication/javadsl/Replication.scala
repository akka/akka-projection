/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Predicate

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityContext
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.japi.function.{ Function => JFunction }
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.ReplicationContextImpl
import akka.persistence.typed.javadsl.ReplicationContext
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.projection.grpc.producer.javadsl.EventProducer
import akka.projection.grpc.producer.javadsl.EventProducerSource
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
   * If combining multiple entity types replicated, or combining with direct usage of
   * Akka Projection gRPC you will have to use the EventProducerService of each of them
   * in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
   */
  def eventProducerService: EventProducerSource

  /**
   * If only replicating one Replicated Event Sourced Entity and not using
   * Akka Projection gRPC this endpoint factory can be used to get a partial function
   * that can be served/bound with an Akka HTTP/2 server
   */
  def createSingleServiceHandler(): JFunction[HttpRequest, CompletionStage[HttpResponse]]

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
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      replicatedBehaviorFactory: JFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
      system: ActorSystem[_]): Replication[Command] = {
    val scalaReplicationSettings = settings.toScala

    val replicatedEntity =
      ReplicatedEntity[Command](
        settings.selfReplicaId,
        settings.configureEntity
          .apply(
            Entity.of(
              settings.entityTypeKey, { (entityContext: EntityContext[Command]) =>
                val replicationId =
                  ReplicationId(entityContext.getEntityTypeKey.name, entityContext.getEntityId, settings.selfReplicaId)
                replicatedBehaviorFactory.apply(
                  factory =>
                    ReplicatedEventSourcing.externalReplication(
                      replicationId,
                      scalaReplicationSettings.otherReplicas.map(_.replicaId) + settings.selfReplicaId)(
                      replicationContext =>
                        factory
                          .apply(replicationContext.asInstanceOf[ReplicationContext])
                          .createEventSourcedBehavior()
                          // MEH
                          .withReplication(replicationContext.asInstanceOf[ReplicationContextImpl])))
              }))
          .toScala)

    val scalaRESOG =
      ReplicationImpl.grpcReplication[Command, Event, State](scalaReplicationSettings, replicatedEntity)(system)
    val jEventProducerSource = new EventProducerSource(
      scalaRESOG.eventProducerService.entityType,
      scalaRESOG.eventProducerService.streamId,
      scalaRESOG.eventProducerService.transformation.toJava,
      scalaRESOG.eventProducerService.settings)

    new Replication[Command] {
      override def eventProducerService: EventProducerSource = jEventProducerSource

      override def createSingleServiceHandler(): JFunction[HttpRequest, CompletionStage[HttpResponse]] =
        EventProducer.grpcServiceHandler(system, jEventProducerSource)

      override def entityTypeKey: EntityTypeKey[Command] =
        scalaRESOG.entityTypeKey.asJava

      override def entityRefFactory: String => EntityRef[Command] =
        (entityId: String) => scalaRESOG.entityRefFactory.apply(entityId).asJava

      override def toString: String = scalaRESOG.toString
    }
  }

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Filter events matching the `producerFilter` predicate, for example based on tags.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  @Deprecated
  @deprecated("Define producerFilter via settings.withProducerFilter", "1.5.1")
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      producerFilter: Predicate[EventEnvelope[Event]],
      replicatedBehaviorFactory: JFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
      system: ActorSystem[_]): Replication[Command] = {
    grpcReplication(settings.withProducerFilter(producerFilter), replicatedBehaviorFactory, system)

  }

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Filter events matching the topic expression according to MQTT specification, including wildcards.
   * The topic of an event is defined by a tag with certain prefix, see `topic-tag-prefix` configuration.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  @Deprecated
  @deprecated("Define topicExpression via settings.withProducerFilterTopicExpression", "1.5.1")
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      topicExpression: String,
      replicatedBehaviorFactory: JFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
      system: ActorSystem[_]): Replication[Command] = {
    grpcReplication(settings.withProducerFilterTopicExpression(topicExpression), replicatedBehaviorFactory, system)
  }

  /**
   * Called to bootstrap the entity on each edge node. In edge mode all connections for replication
   * comes from edge node to cloud. Cloud service needs to be configured for regular grpc replication
   * with edge replication enabled through `ReplicationSettings#withEdgeReplication(true)`.
   */
  def grpcEdgeReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): EdgeReplication[Command] = {
    val scalaReplicationSettings = settings.toScala

    val replicatedEntity =
      ReplicatedEntity[Command](
        settings.selfReplicaId,
        settings.configureEntity
          .apply(
            Entity.of(
              settings.entityTypeKey, { (entityContext: EntityContext[Command]) =>
                val replicationId =
                  ReplicationId(entityContext.getEntityTypeKey.name, entityContext.getEntityId, settings.selfReplicaId)
                replicatedBehaviorFactory.apply(
                  factory =>
                    ReplicatedEventSourcing.externalReplication(
                      replicationId,
                      scalaReplicationSettings.otherReplicas.map(_.replicaId) + settings.selfReplicaId)(
                      replicationContext =>
                        factory
                          .apply(replicationContext.asInstanceOf[ReplicationContext])
                          .createEventSourcedBehavior()
                          // MEH
                          .withReplication(replicationContext.asInstanceOf[ReplicationContextImpl])))
              }))
          .toScala)

    val scalaRESOG =
      ReplicationImpl.grpcReplication[Command, Event, State](scalaReplicationSettings, replicatedEntity)(system)

    new EdgeReplication[Command] {
      override def entityTypeKey: EntityTypeKey[Command] =
        scalaRESOG.entityTypeKey.asJava

      override def entityRefFactory: String => EntityRef[Command] =
        (entityId: String) => scalaRESOG.entityRefFactory.apply(entityId).asJava

      override def toString: String = scalaRESOG.toString
    }
  }

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
