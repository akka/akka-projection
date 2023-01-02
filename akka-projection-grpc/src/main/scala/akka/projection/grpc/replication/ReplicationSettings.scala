/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.producer.EventProducerSettings
import akka.util.JavaDurationConverters._
import com.google.protobuf.Descriptors

import scala.collection.immutable
import scala.reflect.ClassTag
import java.util.{ Set => JSet }
import java.time.{ Duration => JDuration }
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

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
   */
  def apply[Command: ClassTag](
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: Set[Replica],
      entityEventReplicationTimeout: FiniteDuration): ReplicationSettings[Command] = {
    val typeKey = EntityTypeKey[Command](entityTypeName)
    new ReplicationSettings[Command](
      selfReplicaId,
      typeKey,
      eventProducerSettings,
      entityTypeName,
      otherReplicas,
      entityEventReplicationTimeout: FiniteDuration,
      Nil // FIXME descriptors from user, do we need them?
    )
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
   */
  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: JSet[Replica],
      entityEventReplicationTimeout: JDuration): ReplicationSettings[Command] = {
    val classTag = ClassTag[Command](commandClass)
    apply(
      entityTypeName,
      selfReplicaId,
      eventProducerSettings,
      otherReplicas.asScala.toSet,
      entityEventReplicationTimeout.asScala)(classTag)
  }
}

final class ReplicationSettings[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: Set[Replica],
    val entityEventReplicationTimeout: FiniteDuration,
    val protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor]) {
  require(
    !otherReplicas.exists(_.replicaId == selfReplicaId),
    s"selfReplicaId [$selfReplicaId] must not be in 'otherReplicas'")
  require(
    (otherReplicas.map(_.replicaId) + selfReplicaId).size == otherReplicas.size + 1,
    s"selfReplicaId and replica ids of the other replicas must be unique, duplicates found: (${otherReplicas.map(
      _.replicaId) + selfReplicaId}")

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
final class Replica private (
    val replicaId: ReplicaId,
    val numberOfConsumers: Int,
    val grpcClientSettings: GrpcClientSettings,
    val additionalRequestMetadata: Option[akka.grpc.scaladsl.Metadata]) {

  /**
   * Scala API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.scaladsl.Metadata) =
    new Replica(replicaId, numberOfConsumers, grpcClientSettings, Some(metadata))

  /**
   * Java API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.javadsl.Metadata) =
    new Replica(replicaId, numberOfConsumers, grpcClientSettings, Some(metadata.asScala))
}
