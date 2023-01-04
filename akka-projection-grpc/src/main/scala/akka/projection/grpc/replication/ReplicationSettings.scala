/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.javadsl.EventProducerInterceptorAdapter
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.projection.grpc.replication.javadsl.ReplicationProjectionProviderAdapter
import akka.util.JavaDurationConverters._
import com.typesafe.config.Config

import java.time.{ Duration => JDuration }
import java.util.{ Set => JSet }
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

@ApiMayChange
object ReplicationSettings {

  /**
   * Scala API: Settings for replicating an entity over gRPC
   *
   * Note: The replica ids and the entity type name is used as id in offset tracking, changing those will replay
   * events from the start.
   *
   * @param entityTypeName A name for the type of replicated entity
   * @param selfReplicaId The replica id of this node, must not be present among 'otherReplicas'
   * @param eventProducerSettings Event producer settings for the event stream published by this replica
   * @param otherReplicas One entry for each remote replica to replicate into this replica
   * @param entityEventReplicationTimeout A timeout for the replication event, needs to be large enough for the time
   *                                      of sending a message across sharding and persisting it in the local replica
   *                                      of an entity. Hitting this timeout means the entire replication stream will
   *                                      back off and restart.
   * @param parallelUpdates Maximum number of parallel updates sent over sharding to the destination entities
   * @param replicationProjectionProvider A factory for the projection used to keep track of offsets when consuming replicated events
   */
  def apply[Command: ClassTag](
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: Set[Replica],
      entityEventReplicationTimeout: FiniteDuration,
      parallelUpdates: Int,
      replicationProjectionProvider: scaladsl.ReplicationProjectionProvider): ReplicationSettings[Command] = {
    val typeKey = EntityTypeKey[Command](entityTypeName)
    new ReplicationSettings[Command](
      selfReplicaId,
      typeKey,
      eventProducerSettings,
      entityTypeName,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      replicationProjectionProvider,
      None)
  }

  /**
   * Scala API: Create settings from config, the system config is expected to contain a block with the entity type key name.
   * Each replica is further expected to have a top level config entry 'akka.grpc.client.[replica-id]' with Akka gRPC
   * client config for reaching the replica from the other replicas.
   */
  def apply[Command](entityTypeName: String, replicationProjectionProvider: scaladsl.ReplicationProjectionProvider)(
      implicit system: ActorSystem[_],
      classTag: ClassTag[Command]): ReplicationSettings[Command] = {
    val config = system.settings.config.getConfig(entityTypeName)
    val selfReplicaId = ReplicaId(config.getString("self-replica-id"))
    val grpcClientFallBack = system.settings.config.getConfig("""akka.grpc.client."*"""")
    val allReplicas = config
      .getConfigList("replicas")
      .asScala
      .toSet
      .map { config: Config =>
        val replicaId = config.getString("replica-id")
        val clientConfig =
          config.getConfig("grpc.client").withFallback(grpcClientFallBack)
        Replica(
          ReplicaId(replicaId),
          numberOfConsumers = config.getInt("number-of-consumers"),
          // so akka.grpc.client.[replica-id]
          grpcClientSettings = GrpcClientSettings.fromConfig(clientConfig)(system))
      }

    ReplicationSettings[Command](
      entityTypeName,
      selfReplicaId,
      EventProducerSettings(system),
      allReplicas.filter(_.replicaId != selfReplicaId),
      config
        .getDuration("entity-event-replication-timeout")
        .asScala,
      config.getInt("parallel-updates"),
      replicationProjectionProvider)
  }

  /**
   * Java API: Create settings from config, the system config is expected to contain a block with the entity type key name.
   * Each replica is further expected to have a top level config entry 'akka.grpc.client.[replica-id]' with Akka gRPC
   * client config for reaching the replica from the other replicas.
   */
  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      replicationProjectionProvider: javadsl.ReplicationProjectionProvider,
      system: ActorSystem[_]): ReplicationSettings[Command] = {
    val classTag: ClassTag[Command] = ClassTag(commandClass)
    apply(entityTypeName, ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))(system, classTag)
  }

  /**
   * Java API: Settings for replicating an entity over gRPC
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
   * @param parallelUpdates Maximum number of parallel updates sent over sharding to the destination entities
   */
  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: JSet[Replica],
      entityEventReplicationTimeout: JDuration,
      parallelUpdates: Int,
      replicationProjectionProvider: javadsl.ReplicationProjectionProvider): ReplicationSettings[Command] = {
    val classTag = ClassTag[Command](commandClass)
    apply(
      entityTypeName,
      selfReplicaId,
      eventProducerSettings,
      otherReplicas.asScala.toSet,
      entityEventReplicationTimeout.asScala,
      parallelUpdates,
      ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))(classTag)
  }
}

@ApiMayChange
final class ReplicationSettings[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: Set[Replica],
    val entityEventReplicationTimeout: FiniteDuration,
    val parallelUpdates: Int,
    private[akka] val projectionProvider: scaladsl.ReplicationProjectionProvider,
    private[akka] val producerInterceptor: Option[EventProducerInterceptor]) {

  require(
    !otherReplicas.exists(_.replicaId == selfReplicaId),
    s"selfReplicaId [$selfReplicaId] must not be in 'otherReplicas'")
  require(
    (otherReplicas.map(_.replicaId) + selfReplicaId).size == otherReplicas.size + 1,
    s"selfReplicaId and replica ids of the other replicas must be unique, duplicates found: (${otherReplicas.map(
      _.replicaId) + selfReplicaId}")

  def withSelfReplicaId(selfReplicaId: ReplicaId): ReplicationSettings[Command] =
    copy(selfReplicaId = selfReplicaId)

  def withEntityTypeKey[T](entityTypeKey: EntityTypeKey[T]): ReplicationSettings[T] =
    new ReplicationSettings[T](
      selfReplicaId,
      entityTypeKey,
      eventProducerSettings,
      streamId,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      projectionProvider,
      producerInterceptor)

  def withEventProducerSettings(eventProducerSettings: EventProducerSettings): ReplicationSettings[Command] =
    copy(eventProducerSettings = eventProducerSettings)

  def withStreamId(streamId: String): ReplicationSettings[Command] =
    copy(streamId = streamId)

  def withOtherReplicas(replicas: Set[Replica]): ReplicationSettings[Command] =
    copy(otherReplicas = replicas)

  def withOtherReplicas(replicas: JSet[Replica]): ReplicationSettings[Command] =
    copy(otherReplicas = replicas.asScala.toSet)

  /**
   * Scala API: Set the timeout for events being completely processed after arriving to a node in the replication stream
   */
  def withEntityEventReplicationTimeout(duration: FiniteDuration): ReplicationSettings[Command] =
    copy(entityEventReplicationTimeout = duration)

  /**
   * Scala API: Set the timeout for events being completely processed after arriving to a node in the replication stream
   */
  def withEntityEventReplicationTimeout(duration: JDuration): ReplicationSettings[Command] =
    copy(entityEventReplicationTimeout = duration.asScala)

  /**
   * Run up to this many parallel updates over sharding. Note however that updates for the same persistence id
   * is always sequential.
   */
  def withParallelUpdates(parallelUpdates: Int): ReplicationSettings[Command] =
    copy(parallelUpdates = parallelUpdates)

  /**
   * Scala API: Change projection provider
   */
  def withProjectionProvider(projectionProvider: scaladsl.ReplicationProjectionProvider): ReplicationSettings[Command] =
    copy(projectionProvider = projectionProvider)

  /**
   * Java API: Change projection provider
   */
  def withProjectionProvider(projectionProvider: javadsl.ReplicationProjectionProvider): ReplicationSettings[Command] =
    copy(projectionProvider = ReplicationProjectionProviderAdapter.toScala(projectionProvider))

  /**
   * Scala API: Add an interceptor to the gRPC event producer for example for authentication of incoming requests
   */
  def withEventProducerInterceptor(interceptor: EventProducerInterceptor): ReplicationSettings[Command] =
    copy(producerInterceptor = Some(interceptor))

  /**
   * Java API: Add an interceptor to the gRPC event producer for example for authentication of incoming requests
   */
  def withEventProducerInterceptor(
      interceptor: akka.projection.grpc.producer.javadsl.EventProducerInterceptor): ReplicationSettings[Command] =
    copy(producerInterceptor = Some(new EventProducerInterceptorAdapter(interceptor)))

  private def copy(
      selfReplicaId: ReplicaId = selfReplicaId,
      entityTypeKey: EntityTypeKey[Command] = entityTypeKey,
      eventProducerSettings: EventProducerSettings = eventProducerSettings,
      streamId: String = streamId,
      otherReplicas: Set[Replica] = otherReplicas,
      entityEventReplicationTimeout: FiniteDuration = entityEventReplicationTimeout,
      parallelUpdates: Int = parallelUpdates,
      projectionProvider: scaladsl.ReplicationProjectionProvider = projectionProvider,
      producerInterceptor: Option[EventProducerInterceptor] = producerInterceptor) =
    new ReplicationSettings[Command](
      selfReplicaId,
      entityTypeKey,
      eventProducerSettings,
      streamId,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      projectionProvider,
      producerInterceptor)

}

object Replica {

  def apply(replicaId: ReplicaId, numberOfConsumers: Int, grpcClientSettings: GrpcClientSettings): Replica =
    new Replica(replicaId, numberOfConsumers, grpcClientSettings, None)

}

/**
 * Describes a specific remote replica, how to connect to identify, connect and consume events from it.
 *
 *
 * @param replicaId          The unique logical identifier of the replica
 * @param numberOfConsumers  How many consumers to start for consuming events from this replica
 * @param grpcClientSettings Settings for how to connect to the replica, host, port, TLS etc.
 */
@ApiMayChange
final class Replica private (
    val replicaId: ReplicaId,
    val numberOfConsumers: Int,
    val grpcClientSettings: GrpcClientSettings,
    val additionalRequestMetadata: Option[akka.grpc.scaladsl.Metadata]) {

  def withReplicaId(replicaId: ReplicaId): Replica =
    copy(replicaId = replicaId)

  def withNumberOfConsumers(numberOfConsumers: Int): Replica =
    copy(numberOfConsumers = numberOfConsumers)

  def withGrpcClientSettings(grpcClientSettings: GrpcClientSettings): Replica =
    copy(grpcClientSettings = grpcClientSettings)

  /**
   * Scala API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.scaladsl.Metadata): Replica =
    copy(additionalRequestMetadata = Some(metadata))

  /**
   * Java API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.javadsl.Metadata): Replica =
    copy(additionalRequestMetadata = Some(metadata.asScala))

  private def copy(
      replicaId: ReplicaId = replicaId,
      numberOfConsumers: Int = numberOfConsumers,
      grpcClientSettings: GrpcClientSettings = grpcClientSettings,
      additionalRequestMetadata: Option[akka.grpc.scaladsl.Metadata] = additionalRequestMetadata) =
    new Replica(replicaId, numberOfConsumers, grpcClientSettings, additionalRequestMetadata)
}
