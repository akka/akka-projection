/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.projection.grpc.replication.internal.ReplicationSettingsImpl
import akka.projection.grpc.replication.scaladsl

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

@ApiMayChange
object ReplicationSettings {

  /**
   * Settings for replicating an entity over gRPC
   *
   * Note: The replica ids and the entity type name is used as id in offset tracking, changing those will replay
   * events from the start.
   *
   * @param entityTypeName                A name for the type of replicated entity
   * @param selfReplicaId                 The replica id of this node, must not be present among 'otherReplicas'
   * @param eventProducerSettings         Event producer settings for the event stream published by this replica
   * @param otherReplicas                 One entry for each remote replica to replicate into this replica
   * @param entityEventReplicationTimeout A timeout for the replication event, needs to be large enough for the time
   *                                      of sending a message across sharding and persisting it in the local replica
   *                                      of an entity. Hitting this timeout means the entire replication stream will
   *                                      back off and restart.
   * @param parallelUpdates               Maximum number of parallel updates sent over sharding to the destination entities
   * @param replicationProjectionProvider A factory for the projection used to keep track of offsets when consuming replicated events
   */
  def apply[Command: ClassTag](
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: Set[Replica],
      entityEventReplicationTimeout: FiniteDuration,
      parallelUpdates: Int,
      replicationProjectionProvider: scaladsl.ReplicationProjectionProvider): ReplicationSettings[Command] =
    ReplicationSettingsImpl(
      entityTypeName,
      selfReplicaId,
      eventProducerSettings,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      replicationProjectionProvider)

  /**
   * Create settings from config, the system config is expected to contain a block with the entity type key name.
   * Each replica is further expected to have a top level config entry 'akka.grpc.client.[replica-id]' with Akka gRPC
   * client config for reaching the replica from the other replicas.
   */
  def apply[Command](entityTypeName: String, replicationProjectionProvider: scaladsl.ReplicationProjectionProvider)(
      implicit system: ActorSystem[_],
      classTag: ClassTag[Command]): ReplicationSettings[Command] =
    ReplicationSettingsImpl(entityTypeName, replicationProjectionProvider)

}

/**
 * Not for user extension
 */
@DoNotInherit
@ApiMayChange
trait ReplicationSettings[Command] {

  def selfReplicaId: ReplicaId
  def entityTypeKey: EntityTypeKey[Command]
  def eventProducerSettings: EventProducerSettings
  def streamId: String
  def otherReplicas: Set[Replica]
  def entityEventReplicationTimeout: FiniteDuration
  def parallelUpdates: Int
  def projectionProvider: ReplicationProjectionProvider
  def eventProducerInterceptor: Option[EventProducerInterceptor]

  def withSelfReplicaId(selfReplicaId: ReplicaId): ReplicationSettings[Command]

  def withEventProducerSettings(eventProducerSettings: EventProducerSettings): ReplicationSettings[Command]

  def withStreamId(streamId: String): ReplicationSettings[Command]

  def withOtherReplicas(replicas: Set[Replica]): ReplicationSettings[Command]

  /**
   * Set the timeout for events being completely processed after arriving to a node in the replication stream
   */
  def withEntityEventReplicationTimeout(duration: FiniteDuration): ReplicationSettings[Command]

  /**
   * Run up to this many parallel updates over sharding. Note however that updates for the same persistence id
   * is always sequential.
   */
  def withParallelUpdates(parallelUpdates: Int): ReplicationSettings[Command]

  /**
   * Change projection provider
   */
  def withProjectionProvider(projectionProvider: ReplicationProjectionProvider): ReplicationSettings[Command]

  /**
   * Add an interceptor to the gRPC event producer for example for authentication of incoming requests
   */
  def withEventProducerInterceptor(interceptor: EventProducerInterceptor): ReplicationSettings[Command]

  /**
   * Allows for changing the settings of the replicated entity, such as stop message, passivation strategy etc.
   */
  def configureEntity(
      configure: Entity[Command, ShardingEnvelope[Command]] => Entity[Command, ShardingEnvelope[Command]])
      : ReplicationSettings[Command]
}
