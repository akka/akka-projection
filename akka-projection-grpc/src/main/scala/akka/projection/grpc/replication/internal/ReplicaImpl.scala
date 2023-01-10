/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.replication.javadsl.{ Replica => JReplica }
import akka.projection.grpc.replication.scaladsl.{ Replica => SReplica }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ReplicaImpl(
    val replicaId: ReplicaId,
    val numberOfConsumers: Int,
    val grpcClientSettings: GrpcClientSettings,
    val additionalQueryRequestMetadata: Option[akka.grpc.scaladsl.Metadata])
    extends JReplica
    with SReplica {

  override def withReplicaId(replicaId: ReplicaId): ReplicaImpl =
    copy(replicaId = replicaId)

  override def withNumberOfConsumers(numberOfConsumers: Int): ReplicaImpl =
    copy(numberOfConsumers = numberOfConsumers)

  override def withGrpcClientSettings(grpcClientSettings: GrpcClientSettings): ReplicaImpl =
    copy(grpcClientSettings = grpcClientSettings)

  /**
   * Scala API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.scaladsl.Metadata): ReplicaImpl =
    copy(additionalQueryRequestMetadata = Some(metadata))

  /**
   * Java API: Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.javadsl.Metadata): ReplicaImpl =
    copy(additionalQueryRequestMetadata = Some(metadata.asScala))

  private def copy(
      replicaId: ReplicaId = replicaId,
      numberOfConsumers: Int = numberOfConsumers,
      grpcClientSettings: GrpcClientSettings = grpcClientSettings,
      additionalQueryRequestMetadata: Option[akka.grpc.scaladsl.Metadata] = additionalQueryRequestMetadata) =
    new ReplicaImpl(replicaId, numberOfConsumers, grpcClientSettings, additionalQueryRequestMetadata)

  override def toScala: SReplica = this
}
