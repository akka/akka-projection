/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.javadsl.{ Entity => JEntity }
import akka.cluster.sharding.typed.javadsl.{ EntityTypeKey => JEntityTypeKey }
import akka.cluster.sharding.typed.scaladsl.{ Entity => SEntity }
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.projection.grpc.producer.javadsl.EventProducerInterceptorAdapter
import akka.projection.grpc.producer.scaladsl.EventProducerInterceptor
import akka.projection.grpc.replication.javadsl.{ Replica => JReplica }
import akka.projection.grpc.replication.javadsl.{ ReplicationProjectionProvider => JReplicationProjectionProvider }
import akka.projection.grpc.replication.javadsl.{ ReplicationSettings => JReplicationSettings }
import akka.projection.grpc.replication.scaladsl.{ Replica => SReplica }
import akka.projection.grpc.replication.scaladsl.{ ReplicationProjectionProvider => SReplicationProjectionProvider }
import akka.projection.grpc.replication.scaladsl.{ ReplicationSettings => SReplicationSettings }
import akka.util.JavaDurationConverters.JavaDurationOps
import akka.util.JavaDurationConverters.ScalaDurationOps
import com.typesafe.config.Config

import java.time.{ Duration => JDuration }
import java.util.{ Set => JSet }
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ReplicationSettingsImpl {

  def apply[Command](entityTypeName: String, replicationProjectionProvider: SReplicationProjectionProvider)(
      implicit system: ActorSystem[_],
      classTag: ClassTag[Command]): ReplicationSettingsImpl[Command] = {
    val config = system.settings.config.getConfig(entityTypeName)
    val selfReplicaId = ReplicaId(config.getString("self-replica-id"))
    val grpcClientFallBack = system.settings.config.getConfig("""akka.grpc.client."*"""")
    val allReplicas: Set[SReplica] = config
      .getConfigList("replicas")
      .asScala
      .toSet
      .map { config: Config =>
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
          consumersOnRole)

      }

    ReplicationSettingsImpl[Command](
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

  def apply[Command: ClassTag](
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: Set[SReplica],
      entityEventReplicationTimeout: FiniteDuration,
      parallelUpdates: Int,
      replicationProjectionProvider: SReplicationProjectionProvider): ReplicationSettingsImpl[Command] = {
    val typeKey = EntityTypeKey[Command](entityTypeName)
    new ReplicationSettingsImpl[Command](
      selfReplicaId,
      typeKey,
      eventProducerSettings,
      entityTypeName,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      replicationProjectionProvider,
      None,
      identity)
  }

}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ReplicationSettingsImpl[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: Set[SReplica],
    val entityEventReplicationTimeout: FiniteDuration,
    val parallelUpdates: Int,
    val projectionProvider: SReplicationProjectionProvider,
    val eventProducerInterceptor: Option[EventProducerInterceptor],
    val configureEntity: SEntity[Command, ShardingEnvelope[Command]] => SEntity[Command, ShardingEnvelope[Command]])
    extends SReplicationSettings[Command]
    with JReplicationSettings[Command] {

  require(
    !otherReplicas.exists(_.replicaId == selfReplicaId),
    s"selfReplicaId [$selfReplicaId] must not be in 'otherReplicas'")
  require(
    (otherReplicas.map(_.replicaId) + selfReplicaId).size == otherReplicas.size + 1,
    s"selfReplicaId and replica ids of the other replicas must be unique, duplicates found: (${otherReplicas.map(
      _.replicaId) + selfReplicaId}")

  override def withSelfReplicaId(selfReplicaId: ReplicaId): ReplicationSettingsImpl[Command] =
    copy(selfReplicaId = selfReplicaId)

  override def withEventProducerSettings(
      eventProducerSettings: EventProducerSettings): ReplicationSettingsImpl[Command] =
    copy(eventProducerSettings = eventProducerSettings)

  override def withStreamId(streamId: String): ReplicationSettingsImpl[Command] =
    copy(streamId = streamId)

  override def withOtherReplicas(replicas: Set[SReplica]): ReplicationSettingsImpl[Command] =
    copy(otherReplicas = replicas)

  override def withOtherReplicas(replicas: JSet[JReplica]): ReplicationSettingsImpl[Command] =
    copy(otherReplicas = replicas.asScala.toSet[JReplica].map(_.toScala))

  override def withEntityEventReplicationTimeout(duration: FiniteDuration): ReplicationSettingsImpl[Command] =
    copy(entityEventReplicationTimeout = duration)

  override def withEntityEventReplicationTimeout(duration: JDuration): ReplicationSettingsImpl[Command] =
    copy(entityEventReplicationTimeout = duration.asScala)

  override def withParallelUpdates(parallelUpdates: Int): ReplicationSettingsImpl[Command] =
    copy(parallelUpdates = parallelUpdates)

  override def withProjectionProvider(
      projectionProvider: SReplicationProjectionProvider): ReplicationSettingsImpl[Command] =
    copy(projectionProvider = projectionProvider)

  override def withProjectionProvider(
      projectionProvider: JReplicationProjectionProvider): ReplicationSettingsImpl[Command] =
    copy(projectionProvider = ReplicationProjectionProviderAdapter.toScala(projectionProvider))

  override def withEventProducerInterceptor(interceptor: EventProducerInterceptor): ReplicationSettingsImpl[Command] =
    copy(producerInterceptor = Some(interceptor))

  override def withEventProducerInterceptor(
      interceptor: akka.projection.grpc.producer.javadsl.EventProducerInterceptor): ReplicationSettingsImpl[Command] =
    copy(producerInterceptor = Some(new EventProducerInterceptorAdapter(interceptor)))

  override def configureEntity(
      configure: SEntity[Command, ShardingEnvelope[Command]] => SEntity[Command, ShardingEnvelope[Command]])
      : ReplicationSettingsImpl[Command] =
    copy(configureEntity = configure)

  override def configureEntity(
      configure: java.util.function.Function[
        JEntity[Command, ShardingEnvelope[Command]],
        JEntity[Command, ShardingEnvelope[Command]]]): ReplicationSettingsImpl[Command] = {
    // FIXME not possible to convert back and forth between scala and java dsls enough here,
    //       I think we'll need separate settings impls for the dsls after all
    ???
  }

  override def getEntityTypeKey: JEntityTypeKey[Command] = entityTypeKey.asJava

  override def getOtherReplicas: JSet[JReplica] = otherReplicas.map(_.asInstanceOf[JReplica]).asJava

  override def getEntityEventReplicationTimeout: JDuration = entityEventReplicationTimeout.asJava

  private def copy(
      selfReplicaId: ReplicaId = selfReplicaId,
      entityTypeKey: EntityTypeKey[Command] = entityTypeKey,
      eventProducerSettings: EventProducerSettings = eventProducerSettings,
      streamId: String = streamId,
      otherReplicas: Set[SReplica] = otherReplicas,
      entityEventReplicationTimeout: FiniteDuration = entityEventReplicationTimeout,
      parallelUpdates: Int = parallelUpdates,
      projectionProvider: SReplicationProjectionProvider = projectionProvider,
      producerInterceptor: Option[EventProducerInterceptor] = eventProducerInterceptor,
      configureEntity: SEntity[Command, ShardingEnvelope[Command]] => SEntity[Command, ShardingEnvelope[Command]] =
        configureEntity) =
    new ReplicationSettingsImpl[Command](
      selfReplicaId,
      entityTypeKey,
      eventProducerSettings,
      streamId,
      otherReplicas,
      entityEventReplicationTimeout,
      parallelUpdates,
      projectionProvider,
      producerInterceptor,
      configureEntity)

  override def toString = s"ReplicationSettings($selfReplicaId, $entityTypeKey, $streamId, $otherReplicas)"
}
