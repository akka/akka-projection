/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.javadsl.EventProducerInterceptor
import akka.projection.grpc.replication.internal.ReplicationProjectionProviderAdapter
import akka.projection.grpc.replication.internal.ReplicationSettingsImpl
import akka.util.JavaDurationConverters.JavaDurationOps

import java.time.Duration
import java.util.{ Set => JSet }
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

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
   * @param replicationProjectionProvider Factory for the projection to use on the consuming side
   * @param parallelUpdates               Maximum number of parallel updates sent over sharding to the destination entities
   */
  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: JSet[Replica],
      entityEventReplicationTimeout: Duration,
      parallelUpdates: Int,
      replicationProjectionProvider: ReplicationProjectionProvider): ReplicationSettings[Command] = {
    val classTag = ClassTag[Command](commandClass)
    akka.projection.grpc.replication.scaladsl.ReplicationSettings
      .apply[Command](
        entityTypeName,
        selfReplicaId,
        eventProducerSettings,
        otherReplicas.asScala.toSet[Replica].map(_.toScala),
        entityEventReplicationTimeout.asScala,
        parallelUpdates,
        ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))(classTag)
      .asInstanceOf[ReplicationSettingsImpl[Command]]
  }

  /**
   * Create settings from config, the system config is expected to contain a block with the entity type key name.
   * Each replica is further expected to have a top level config entry 'akka.grpc.client.[replica-id]' with Akka gRPC
   * client config for reaching the replica from the other replicas.
   */
  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      replicationProjectionProvider: ReplicationProjectionProvider,
      system: ActorSystem[_]): ReplicationSettings[Command] = {
    val classTag: ClassTag[Command] = ClassTag(commandClass)
    akka.projection.grpc.replication.scaladsl.ReplicationSettings
      .apply(entityTypeName, ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))(
        system,
        classTag)
      .asInstanceOf[ReplicationSettingsImpl[Command]]
  }
}

/**
 * Not for user extension, construct using ReplicationSettings#create
 */
@ApiMayChange
@DoNotInherit
trait ReplicationSettings[Command] {

  def selfReplicaId: ReplicaId

  def getEntityTypeKey: EntityTypeKey[Command]

  def eventProducerSettings: EventProducerSettings

  def streamId: String

  def getOtherReplicas: JSet[Replica]

  def getEntityEventReplicationTimeout: Duration

  def parallelUpdates: Int

  def withSelfReplicaId(selfReplicaId: ReplicaId): ReplicationSettings[Command]

  def withEventProducerSettings(eventProducerSettings: EventProducerSettings): ReplicationSettings[Command]

  def withStreamId(streamId: String): ReplicationSettings[Command]

  def withOtherReplicas(replicas: JSet[Replica]): ReplicationSettings[Command]

  /**
   * Set the timeout for events being completely processed after arriving to a node in the replication stream
   */
  def withEntityEventReplicationTimeout(duration: Duration): ReplicationSettings[Command]

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
      configure: java.util.function.Function[
        Entity[Command, ShardingEnvelope[Command]],
        Entity[Command, ShardingEnvelope[Command]]]): ReplicationSettings[Command]

}
