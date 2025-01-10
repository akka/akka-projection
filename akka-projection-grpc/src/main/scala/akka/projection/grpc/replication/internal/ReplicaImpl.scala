/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.annotation.InternalApi
import akka.grpc.GrpcClientSettings
import akka.grpc.scaladsl.Metadata
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.replication.javadsl.{ Replica => JReplica }
import akka.projection.grpc.replication.scaladsl.{ Replica => SReplica }

import java.util.Optional
import scala.jdk.OptionConverters._

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final case class ReplicaImpl(
    override val replicaId: ReplicaId,
    override val numberOfConsumers: Int,
    override val grpcClientSettings: GrpcClientSettings,
    override val additionalQueryRequestMetadata: Option[akka.grpc.scaladsl.Metadata],
    override val consumersOnClusterRole: Option[String])
    extends JReplica
    with SReplica {

  def getAdditionalQueryRequestMetadata: Optional[Metadata] = additionalQueryRequestMetadata.toJava

  def getConsumersOnClusterRole: Optional[String] = consumersOnClusterRole.toJava

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

  def withConsumersOnClusterRole(clusterRole: String): ReplicaImpl =
    copy(consumersOnClusterRole = Some(clusterRole))

  override def toScala: SReplica = this

  override def toString: String = s"Replica($replicaId, $numberOfConsumers, ${consumersOnClusterRole.getOrElse("")})"
}
