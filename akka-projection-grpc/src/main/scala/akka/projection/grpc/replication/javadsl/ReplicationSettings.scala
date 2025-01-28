/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import java.util.function.{ Function => JFunction }
import java.util.{ List => JList }
import akka.actor.typed.ActorSystem
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

import com.typesafe.config.Config

import java.time.Duration
import java.util.Collections
import java.util.Optional
import java.util.function.Predicate
import java.util.{ Set => JSet }
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.internal.TopicMatcher
import akka.projection.grpc.replication.internal.ReplicationProjectionProviderAdapter

import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.jdk.DurationConverters._

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
      edgeReplication = false,
      configureEntity = identity,
      producerFilter = _ => true,
      initialConsumerFilter = Collections.emptyList,
      // no system config to get defaults from, repeating config file defaults
      edgeReplicationDeliveryRetries = 3,
      edgeReplicationDeliveryMinBackoff = 250.millis.toJava,
      edgeReplicationDeliveryMaxBackoff = 5.seconds.toJava)
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

    // global config only for some things
    val replicationConfig = system.settings.config.getConfig("akka.projection.grpc.replication")

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
      edgeReplication = false,
      configureEntity = identity,
      producerFilter = _ => true,
      initialConsumerFilter = Collections.emptyList,
      edgeReplicationDeliveryRetries = replicationConfig.getInt("edge-replication-delivery-retries"),
      edgeReplicationDeliveryMinBackoff = replicationConfig.getDuration("edge-replication-delivery-min-backoff"),
      edgeReplicationDeliveryMaxBackoff = replicationConfig.getDuration("edge-replication-delivery-max-backoff"))
  }
}

/**
 * Not for user extension, construct using ReplicationSettings#create
 */
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
    val edgeReplication: Boolean,
    val configureEntity: JFunction[
      Entity[Command, ShardingEnvelope[Command]],
      Entity[Command, ShardingEnvelope[Command]]],
    val producerFilter: Predicate[EventEnvelope[Any]],
    val initialConsumerFilter: JList[ConsumerFilter.FilterCriteria],
    val edgeReplicationDeliveryRetries: Int,
    val edgeReplicationDeliveryMinBackoff: Duration,
    val edgeReplicationDeliveryMaxBackoff: Duration) {

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
      configure: JFunction[Entity[Command, ShardingEnvelope[Command]], Entity[Command, ShardingEnvelope[Command]]])
      : ReplicationSettings[Command] =
    copy(configureEntity = configure)

  /**
   * Filter events matching the `producerFilter` predicate, for example based on tags.
   */
  def withProducerFilter[Event](producerFilter: Predicate[EventEnvelope[Event]]): ReplicationSettings[Command] =
    copy(producerFilter = producerFilter.asInstanceOf[Predicate[EventEnvelope[Any]]])

  /**
   * Filter events matching the topic expression according to MQTT specification, including wildcards.
   * The topic of an event is defined by a tag with certain prefix, see `topic-tag-prefix` configuration.
   */
  def withProducerFilterTopicExpression(topicExpression: String): ReplicationSettings[Command] = {
    val topicMatcher = TopicMatcher(topicExpression)
    withProducerFilter[Any](env => topicMatcher.matches(env, eventProducerSettings.topicTagPrefix))
  }

  /**
   * Set the initial consumer filter to use for events. Should only be used for static, up front consumer filters.
   * Combining this with updating consumer filters directly means that the filters may be reset to these
   * filters.
   */
  def withInitialConsumerFilter(
      initialConsumerFilter: JList[ConsumerFilter.FilterCriteria]): ReplicationSettings[Command] =
    copy(initialConsumerFilter = initialConsumerFilter)

  def withEdgeReplication(edgeReplication: Boolean): ReplicationSettings[Command] = {
    copy(edgeReplication = edgeReplication)
  }

  /**
   * Replicated event sourcing from edge sends each event over sharding, in case that delivery
   * fails or times out, retry this number of times
   */
  def withEdgeReplicationDeliveryRetries(retries: Int): ReplicationSettings[Command] =
    copy(edgeReplicationDeliveryRetries = retries)

  def withEdgeReplicationDeliveryMinBackoff(minBackoff: Duration): ReplicationSettings[Command] =
    copy(edgeReplicationDeliveryMinBackoff = minBackoff)

  def withEdgeReplicationDeliveryMaxBackoff(maxBackoff: Duration): ReplicationSettings[Command] =
    copy(edgeReplicationDeliveryMaxBackoff = maxBackoff)

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
      edgeReplication: Boolean = edgeReplication,
      configureEntity: JFunction[
        Entity[Command, ShardingEnvelope[Command]],
        Entity[Command, ShardingEnvelope[Command]]] = configureEntity,
      producerFilter: Predicate[EventEnvelope[Any]] = producerFilter,
      initialConsumerFilter: JList[ConsumerFilter.FilterCriteria] = initialConsumerFilter,
      edgeReplicationDeliveryRetries: Int = edgeReplicationDeliveryRetries,
      edgeReplicationDeliveryMinBackoff: Duration = edgeReplicationDeliveryMinBackoff,
      edgeReplicationDeliveryMaxBackoff: Duration = edgeReplicationDeliveryMaxBackoff): ReplicationSettings[Command] =
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
      edgeReplication,
      configureEntity,
      producerFilter,
      initialConsumerFilter,
      edgeReplicationDeliveryRetries,
      edgeReplicationDeliveryMinBackoff,
      edgeReplicationDeliveryMaxBackoff)

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
      entityEventReplicationTimeout = entityEventReplicationTimeout.toScala,
      parallelUpdates = parallelUpdates,
      replicationProjectionProvider = ReplicationProjectionProviderAdapter.toScala(replicationProjectionProvider))
      .withProducerFilter(producerFilter.test)
      .withEdgeReplication(edgeReplication)
      .withInitialConsumerFilter(initialConsumerFilter.asScala.toVector)
      .withEdgeReplicationDeliveryRetries(edgeReplicationDeliveryRetries)
      .withEdgeReplicationDeliveryMinBackoff(edgeReplicationDeliveryMinBackoff.toScala)
      .withEdgeReplicationDeliveryMaxBackoff(edgeReplicationDeliveryMaxBackoff.toScala)

}
