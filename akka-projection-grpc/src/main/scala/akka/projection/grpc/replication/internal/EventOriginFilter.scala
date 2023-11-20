/*
 * Copyright (C) 2023 Lightbend Inc. <https://www.lightbend.com>
 */
package akka.projection.grpc.replication.internal

import akka.annotation.InternalApi
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.projection.grpc.internal.proto.InitReq

/**
 * INTERNAL API
 */
@InternalApi private[akka] class EventOriginFilter(selfReplicaId: ReplicaId) {

  def createFilter(initReq: InitReq): EventEnvelope[_] => Boolean = {
    // Events originating from other replicas that the consumer is connected to are excluded
    // because the consumer will receive them directly from the other replica.
    // Events originating from the consumer replica itself are excluded (break the cycle).
    // Events originating from the producer replica are always included.
    val exclude: Set[ReplicaId] =
      initReq.otherReplicaIds.map(ReplicaId.apply).toSet ++
      (if (initReq.replicaId == "") Nil else List(ReplicaId(initReq.replicaId))) -
      selfReplicaId

    { envelope =>
      // Events from backtracking are lazily loaded via `loadEvent` if needed.
      // Filter is done via `loadEvent` in that case.
      envelope.eventMetadata match {
        case Some(meta: ReplicatedEventMetadata) =>
          !exclude(meta.originReplica)
        case _ =>
          // FIXME eventMetadata isn't loaded by backtracking
          throw new IllegalArgumentException(
            s"Got an event without replication metadata, not supported (pid: ${envelope.persistenceId}, seq_nr: ${envelope.sequenceNr})")
      }
    }
  }

}
