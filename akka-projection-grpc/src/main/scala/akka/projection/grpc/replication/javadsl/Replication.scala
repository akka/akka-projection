/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://akka.io>
 */

package akka.projection.grpc.replication.javadsl

import java.util.concurrent.CompletionStage
import java.util.function.Predicate
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.annotation.DoNotInherit
import akka.japi.function.{ Function => JApiFunction }
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityContext
import akka.cluster.sharding.typed.javadsl.EntityRef
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.grpc.javadsl.ServiceHandler
import akka.http.javadsl.model.HttpRequest
import akka.http.javadsl.model.HttpResponse
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.ReplicationContextImpl
import akka.persistence.typed.javadsl.ReplicationContext
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.projection.grpc.consumer.javadsl.EventProducerPushDestination
import akka.projection.grpc.producer.javadsl.EventProducer
import akka.projection.grpc.producer.javadsl.EventProducerSource
import akka.projection.grpc.replication.internal.ReplicationImpl

import java.util.function.{ Function => JFunction }
import java.util.Optional
import scala.jdk.OptionConverters._

/**
 * Created using [[Replication.grpcReplication]], which starts sharding with the entity and
 * replication stream consumers but not the replication endpoint needed to publish events to other replication places.
 *
 * @tparam Command The type of commands the Replicated Event Sourced Entity accepts
 *
 * Not for user extension
 */
@DoNotInherit
trait Replication[Command] {

  @deprecated("Use eventProducerSource instead", "1.5.1")
  def eventProducerService: EventProducerSource

  /**
   * If combining multiple entity types replicated, or combining with direct usage of
   * Akka Projection gRPC you will have to use the EventProducerService of each of them
   * in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
   */
  def eventProducerSource: EventProducerSource

  /**
   * Scala API: Push destinations for accepting/combining multiple Replicated Event Sourced entity types
   * and possibly also regular projections into one producer push destination handler in a set passed to
   * EventProducerPushDestination.grpcServiceHandler to create a single gRPC endpoint.
   */
  def eventProducerPushDestination: Optional[EventProducerPushDestination]

  /**
   * If only replicating one Replicated Event Sourced Entity and not using
   * Akka Projection gRPC this endpoint factory can be used to get a partial function
   * that can be served/bound with an Akka HTTP/2 server
   */
  def createSingleServiceHandler(): JApiFunction[HttpRequest, CompletionStage[HttpResponse]]

  /**
   * Entity type key for looking up the entities
   */
  def entityTypeKey: EntityTypeKey[Command]

  /**
   * Shortcut for creating EntityRefs for the sharded Replicated Event Sourced entities for
   * sending commands.
   */
  def entityRefFactory: JFunction[String, EntityRef[Command]]
}

object Replication {

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      replicatedBehaviorFactory: JApiFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
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
                          .withReplication(replicationContext.asInstanceOf[ReplicationContextImpl])))
              }))
          .toScala)

    val scalaReplication =
      ReplicationImpl.grpcReplication[Command, Event, State](scalaReplicationSettings, replicatedEntity)(system)
    val jEventProducerSource = EventProducerSource.fromScala(scalaReplication.eventProducerSource)

    val jEventProducerPushDestination =
      scalaReplication.eventProducerPushDestination.map(EventProducerPushDestination.fromScala).toJava
    new Replication[Command] {
      override def eventProducerService: EventProducerSource = jEventProducerSource

      override def eventProducerSource: EventProducerSource = jEventProducerSource

      override def eventProducerPushDestination: Optional[EventProducerPushDestination] =
        jEventProducerPushDestination

      override def createSingleServiceHandler(): JApiFunction[HttpRequest, CompletionStage[HttpResponse]] = {
        val handler = EventProducer.grpcServiceHandler(system, jEventProducerSource)
        if (jEventProducerPushDestination.isPresent) {
          // Fold in edge push gRPC consumer service if enabled
          val eventProducerPushHandler =
            EventProducerPushDestination.grpcServiceHandler(jEventProducerPushDestination.get(), system)
          ServiceHandler.concatOrNotFound(handler, eventProducerPushHandler)
        } else handler
      }

      override def entityTypeKey: EntityTypeKey[Command] =
        scalaReplication.entityTypeKey.asJava

      override def entityRefFactory: JFunction[String, EntityRef[Command]] =
        (entityId: String) => scalaReplication.entityRefFactory.apply(entityId).asJava

      override def toString: String = scalaReplication.toString
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
      replicatedBehaviorFactory: JApiFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
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
      replicatedBehaviorFactory: JApiFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
      system: ActorSystem[_]): Replication[Command] = {
    grpcReplication(settings.withProducerFilterTopicExpression(topicExpression), replicatedBehaviorFactory, system)
  }

  /**
   * Called to bootstrap the entity on each edge node. In edge mode all connections for replication
   * comes from edge node to cloud. Cloud service needs to be configured for regular grpc replication
   * with edge replication enabled through `ReplicationSettings#withEdgeReplication(true)`.
   *
   * Each edge replica must use a unique replica id.
   *
   * The cloud replicas does not know about the edge replica ids up front (it should not be in their "other replicas" set).
   *
   * An edge replica can connect to more than one cloud replica for redundancy (but only one is required).
   */
  def grpcEdgeReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      replicatedBehaviorFactory: JApiFunction[ReplicatedBehaviors[Command, Event, State], Behavior[Command]],
      system: ActorSystem[_]): EdgeReplication[Command] = {
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
                          .withReplication(replicationContext.asInstanceOf[ReplicationContextImpl])))
              }))
          .toScala)

    val scalaEdgeReplication =
      ReplicationImpl.grpcEdgeReplication[Command](scalaReplicationSettings, replicatedEntity)(system)

    new EdgeReplication[Command] {
      override def entityTypeKey: EntityTypeKey[Command] =
        scalaEdgeReplication.entityTypeKey.asJava

      override def entityRefFactory: JFunction[String, EntityRef[Command]] =
        (entityId: String) => scalaEdgeReplication.entityRefFactory.apply(entityId).asJava

      override def toString: String = scalaEdgeReplication.toString
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
  def entityRefFactory: JFunction[String, EntityRef[Command]]
}
