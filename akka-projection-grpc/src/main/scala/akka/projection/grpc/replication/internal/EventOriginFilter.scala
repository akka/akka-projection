/*
 * Copyright (C) 2023-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.projection.grpc.internal.proto.ReplicaInfo

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventOriginFilter(selfReplicaId: ReplicaId) {

  def createFilter(consumerReplicaInfo: ReplicaInfo): EventEnvelope[_] => Boolean = {
    // For edge topologies, like star topologies, an edge replica is not connected
    // to all other replicas, but should be able to receive events indirectly via
    // the replica that it is consuming from.
    //
    // Events originating from other replicas that the consumer is connected to are excluded
    // because the consumer will receive them directly from the other replica.
    // Events originating from the consumer replica itself are excluded (break the cycle).
    // Events originating from the producer replica are always included.
    val exclude: Set[ReplicaId] =
      consumerReplicaInfo.otherReplicaIds.map(ReplicaId.apply).toSet ++
      (if (consumerReplicaInfo.replicaId == "") Nil else List(ReplicaId(consumerReplicaInfo.replicaId))) -
      selfReplicaId

    { envelope =>
      // eventMetadata is not included in backtracking envelopes.
      // Events from backtracking are lazily loaded via `loadEvent` if needed.
      // Filter is done via `loadEvent` in that case.
      if (envelope.eventOption.isEmpty)
        true
      else
        envelope.metadata[ReplicatedEventMetadata] match {
          case Some(meta) =>
            !exclude(meta.originReplica)
          case None =>
            throw new IllegalArgumentException(
              s"Got an event without replication metadata, not supported (pid: ${envelope.persistenceId}, seq_nr: ${envelope.sequenceNr})")
        }
    }
  }

}
