/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.javadsl.Entity
import akka.cluster.sharding.typed.javadsl.EntityTypeKey
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.javadsl.EventProducerInterceptor
import akka.projection.grpc.replication.internal.ReplicaImpl
import akka.projection.grpc.replication.scaladsl.{ ReplicationSettings => SReplicationSettings }
import akka.util.JavaDurationConverters.JavaDurationOps
import com.typesafe.config.Config

import java.time.Duration
import java.util.Optional
import java.util.{ Set => JSet }
import scala.jdk.CollectionConverters._
import akka.projection.grpc.replication.internal.ReplicationProjectionProviderAdapter

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
   * @param replicas                      One entry for each replica to replicate into this replica (if it contains self replica id that is filtered out)
   *                                      to create the `otherReplicas` set.
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
      replicas: JSet[Replica],
      entityEventReplicationTimeout: Duration,
      parallelUpdates: Int,
      replicationProjectionProvider: ReplicationProjectionProvider): ReplicationSettings[Command] = {
    val entityTypeKey = EntityTypeKey.create(commandClass, entityTypeName)
    val otherReplicas = replicas.asScala.filter(_.replicaId != selfReplicaId).asJava
    new ReplicationSettings[Command](
      selfReplicaId,
      entityTypeKey,
      eventProducerSettings,
      entityTypeName,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      replicationProjectionProvider,
      Optional.empty(),
      identity)
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
    val config = system.settings.config.getConfig(entityTypeName)
    val selfReplicaId = ReplicaId(config.getString("self-replica-id"))
    val grpcClientFallBack = system.settings.config.getConfig("""akka.grpc.client."*"""")
    val allReplicas: Set[Replica] = config
      .getConfigList("replicas")
      .asScala
      .toSet
      .map { (config: Config) =>
        val replicaId = config.getString("replica-id")
        val clientConfig =
          config.getConfig("grpc.client").withFallback(grpcClientFallBack)

        val consumersOnRole =
          if (config.hasPath("consumers-on-cluster-role")) Some(config.getString("consumers-on-cluster-role"))
          else None
        new ReplicaImpl(
          ReplicaId(replicaId),
          numberOfConsumers = config.getInt("number-of-consumers"),
          // so akka.grpc.client.[replica-id]
          grpcClientSettings = GrpcClientSettings.fromConfig(clientConfig)(system),
          None,
          consumersOnRole): Replica
      }
    val otherReplicas = allReplicas.filter(_.replicaId != selfReplicaId).asJava
    val entityTypeKey = EntityTypeKey.create(commandClass, entityTypeName)

    new ReplicationSettings[Command](
      selfReplicaId = selfReplicaId,
      entityTypeKey = entityTypeKey,
      eventProducerSettings = EventProducerSettings(system),
      streamId = entityTypeName,
      otherReplicas = otherReplicas,
      entityEventReplicationTimeout = config
        .getDuration("entity-event-replication-timeout"),
      parallelUpdates = config.getInt("parallel-updates"),
      replicationProjectionProvider = replicationProjectionProvider,
      Optional.empty(),
      identity)
  }
}

/**
 * Not for user extension, construct using ReplicationSettings#create
 */
@ApiMayChange
@DoNotInherit
final class ReplicationSettings[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: JSet[Replica],
    val entityEventReplicationTimeout: Duration,
    val parallelUpdates: Int,
    val replicationProjectionProvider: ReplicationProjectionProvider,
    val eventProducerInterceptor: Optional[EventProducerInterceptor],
    val configureEntity: java.util.function.Function[
      Entity[Command, ShardingEnvelope[Command]],
      Entity[Command, ShardingEnvelope[Command]]]) {

  def withSelfReplicaId(selfReplicaId: ReplicaId): ReplicationSettings[Command] =
    copy(selfReplicaId = selfReplicaId)

  def withEventProducerSettings(eventProducerSettings: EventProducerSettings): ReplicationSettings[Command] =
    copy(eventProducerSettings = eventProducerSettings)

  def withStreamId(streamId: String): ReplicationSettings[Command] =
    copy(streamId = streamId)

  def withOtherReplicas(otherReplicas: JSet[Replica]): ReplicationSettings[Command] =
    copy(otherReplicas = otherReplicas)

  /**
   * Set the timeout for events being completely processed after arriving to a node in the replication stream
   */
  def withEntityEventReplicationTimeout(duration: Duration): ReplicationSettings[Command] =
    copy(entityEventReplicationTimeout = duration)

  /**
   * Run up to this many parallel updates over sharding. Note however that updates for the same persistence id
   * is always sequential.
   */
  def withParallelUpdates(parallelUpdates: Int): ReplicationSettings[Command] =
    copy(parallelUpdates = parallelUpdates)

  /**
   * Change projection provider
   */
  def withProjectionProvider(projectionProvider: ReplicationProjectionProvider): ReplicationSettings[Command] =
    copy(projectionProvider = projectionProvider)

  /**
   * Add an interceptor to the gRPC event producer for example for authentication of incoming requests
   */
  def withEventProducerInterceptor(interceptor: EventProducerInterceptor): ReplicationSettings[Command] =
    copy(eventProducerInterceptor = Optional.of(interceptor))

  /**
   * Allows for changing the settings of the replicated entity, such as stop message, passivation strategy etc.
   */
  def configureEntity(
      configure: java.util.function.Function[
        Entity[Command, ShardingEnvelope[Command]],
        Entity[Command, ShardingEnvelope[Command]]]): ReplicationSettings[Command] =
    copy(configureEntity = configure)

  private def copy(
      selfReplicaId: ReplicaId = selfReplicaId,
      entityTypeKey: EntityTypeKey[Command] = entityTypeKey,
      eventProducerSettings: EventProducerSettings = eventProducerSettings,
      streamId: String = streamId,
      otherReplicas: JSet[Replica] = otherReplicas,
      entityEventReplicationTimeout: Duration = entityEventReplicationTimeout,
      parallelUpdates: Int = parallelUpdates,
      projectionProvider: ReplicationProjectionProvider = replicationProjectionProvider,
      eventProducerInterceptor: Optional[EventProducerInterceptor] = eventProducerInterceptor,
      configureEntity: java.util.function.Function[
        Entity[Command, ShardingEnvelope[Command]],
        Entity[Command, ShardingEnvelope[Command]]] = configureEntity): ReplicationSettings[Command] =
    new ReplicationSettings[Command](
      selfReplicaId,
      entityTypeKey,
      eventProducerSettings,
      streamId,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      projectionProvider,
      eventProducerInterceptor,
      configureEntity)

  override def toString =
    s"ReplicationSettings($selfReplicaId, $entityTypeKey, $streamId, ${otherReplicas.asScala.mkString(", ")})"

  /**
   * INTERNAL API
   */
  @InternalApi
  private[akka] def toScala: SReplicationSettings[Command] =
    SReplicationSettings[Command](
      entityTypeKey = entityTypeKey.asScala,
      selfReplicaId = selfReplicaId,
      eventProducerSettings = eventProducerSettings,
      otherReplicas = otherReplicas.asScala.map(_.toScala).toSet,
      entityEventReplicationTimeout = entityEventReplicationTimeout.asScala,
      parallelUpdates = parallelUpdates,
      replicationProjectionProvider = ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))

}
