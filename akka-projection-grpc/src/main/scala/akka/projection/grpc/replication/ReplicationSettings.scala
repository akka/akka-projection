/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication

import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.producer.EventProducerSettings
import com.google.protobuf.Descriptors

import scala.collection.immutable
import scala.reflect.ClassTag
import java.util.{ Set => JSet }
import scala.jdk.CollectionConverters._

object ReplicationSettings {

  /**
   * Scala API: Settings for replicating an entity over gRPC
   */
  def apply[Command: ClassTag](
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: Set[Replica]): ReplicationSettings[Command] = {
    val typeKey = EntityTypeKey[Command](entityTypeName)
    new ReplicationSettings[Command](
      selfReplicaId,
      typeKey,
      eventProducerSettings,
      entityTypeName,
      otherReplicas,
      Nil // FIXME descriptors from user, do we need them?
    )
  }

  def create[Command](
      commandClass: Class[Command],
      entityTypeName: String,
      selfReplicaId: ReplicaId,
      eventProducerSettings: EventProducerSettings,
      otherReplicas: JSet[Replica]): ReplicationSettings[Command] = {
    val classTag = ClassTag[Command](commandClass)
    apply(entityTypeName, selfReplicaId, eventProducerSettings, otherReplicas.asScala.toSet)(classTag)
  }
}

final class ReplicationSettings[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: Set[Replica],
    val protobufDescriptors: immutable.Seq[Descriptors.FileDescriptor]) {
  require(
    !otherReplicas.exists(_.replicaId == selfReplicaId),
    s"selfReplicaId [$selfReplicaId] must not be in 'otherReplicas'")
  // FIXME verify that replica ids align?

}

object Replica {

  def apply(
      replicaId: ReplicaId,
      numberOfConsumers: Int,
      querySettings: GrpcQuerySettings,
      grpcClientSettings: GrpcClientSettings): Replica =
    new Replica(replicaId, numberOfConsumers, querySettings, grpcClientSettings)

}

/**
 * Describes a specific remote replica, how to connect to identify, connect and consume events from it.
 *
 *
 * @param replicaId          The unique logical identifier of the replica
 * @param numberOfConsumers  How many consumers to start for consuming events from this replica
 * @param querySettings      Additional query settings for consuming events from the replica
 * @param grpcClientSettings Settings for how to connect to the replica, host, port, TLS etc.
 */
final class Replica private (
    val replicaId: ReplicaId,
    val numberOfConsumers: Int,
    val grpcQuerySettings: GrpcQuerySettings,
    val grpcClientSettings: GrpcClientSettings)
