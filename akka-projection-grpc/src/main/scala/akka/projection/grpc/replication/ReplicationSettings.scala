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

import scala.reflect.ClassTag

object ReplicationSettings {
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
      Seq.empty // FIXME descriptors from user, do we need them?
    )
  }
}

final class ReplicationSettings[Command] private (
    val selfReplicaId: ReplicaId,
    val entityTypeKey: EntityTypeKey[Command],
    val eventProducerSettings: EventProducerSettings,
    val streamId: String,
    val otherReplicas: Set[Replica],
    val protobufDescriptors: Seq[Descriptors.FileDescriptor]) {
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

final class Replica private (
    val replicaId: ReplicaId,
    val numberOfConsumers: Int,
    val grpcQuerySettings: GrpcQuerySettings,
    val grpcClientSettings: GrpcClientSettings)
