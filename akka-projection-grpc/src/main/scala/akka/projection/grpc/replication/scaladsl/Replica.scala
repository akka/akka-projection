/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.annotation.DoNotInherit
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.replication.internal.ReplicaImpl

object Replica {

  /**
   * Describes a specific remote replica, how to connect to identify, connect and consume events from it.
   *
   * @param replicaId          The unique logical identifier of the replica
   * @param numberOfConsumers  How many consumers to start for consuming events from this replica
   * @param grpcClientSettings Settings for how to connect to the replica, host, port, TLS etc.
   */
  def apply(replicaId: ReplicaId, numberOfConsumers: Int, grpcClientSettings: GrpcClientSettings): Replica =
    new ReplicaImpl(replicaId, numberOfConsumers, grpcClientSettings, None, None)

}

/**
 * Not for user extension, construct using [[Replica.apply]]
 */
@DoNotInherit
trait Replica {
  def replicaId: ReplicaId
  def numberOfConsumers: Int
  def grpcClientSettings: GrpcClientSettings
  def additionalQueryRequestMetadata: Option[akka.grpc.scaladsl.Metadata]
  def consumersOnClusterRole: Option[String]

  def withReplicaId(replicaId: ReplicaId): Replica
  def withNumberOfConsumers(numberOfConsumers: Int): Replica
  def withGrpcClientSettings(grpcClientSettings: GrpcClientSettings): Replica

  /**
   * Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.scaladsl.Metadata): Replica

  /**
   * Only run consumers for this replica on cluster nodes with this role
   */
  def withConsumersOnClusterRole(clusterRole: String): Replica

}
