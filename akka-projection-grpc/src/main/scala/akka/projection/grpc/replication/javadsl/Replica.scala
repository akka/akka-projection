/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.annotation.DoNotInherit
import akka.grpc.GrpcClientSettings
import akka.persistence.typed.ReplicaId
import akka.projection.grpc.replication.internal.ReplicaImpl

import java.util.Optional

object Replica {

  /**
   * Describes a specific remote replica, how to connect to identify, connect and consume events from it.
   *
   * @param replicaId          The unique logical identifier of the replica
   * @param numberOfConsumers  How many consumers to start for consuming events from this replica
   * @param grpcClientSettings Settings for how to connect to the replica, host, port, TLS etc.
   */
  def create(replicaId: ReplicaId, numberOfConsumers: Int, grpcClientSettings: GrpcClientSettings): Replica =
    new ReplicaImpl(replicaId, numberOfConsumers, grpcClientSettings, None, None)

}

/**
 * Not for user extension, construct using Replica#create
 */
@DoNotInherit
trait Replica {

  def replicaId: ReplicaId

  def withReplicaId(replicaId: ReplicaId): Replica

  def withNumberOfConsumers(numberOfConsumers: Int): Replica

  def numberOfConsumers: Int

  def grpcClientSettings: GrpcClientSettings

  def getAdditionalQueryRequestMetadata: Optional[akka.grpc.scaladsl.Metadata]

  def getConsumersOnClusterRole: Optional[String]

  def withGrpcClientSettings(grpcClientSettings: GrpcClientSettings): Replica

  /**
   * Metadata to include in the requests to the remote Akka gRPC projection endpoint
   */
  def withAdditionalQueryRequestMetadata(metadata: akka.grpc.javadsl.Metadata): Replica

  /**
   * Only run consumers for this replica on cluster nodes with this role
   */
  def withConsumersOnClusterRole(clusterRole: String): Replica

  def toScala: akka.projection.grpc.replication.scaladsl.Replica
}
