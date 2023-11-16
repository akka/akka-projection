package akka.projection.grpc.replication.internal

import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.persistence.query.typed.EventEnvelope
import akka.projection.grpc.replication.scaladsl.ReplicationSettings

/**
 * INTERNAL API
 */
private[akka] object PushReplicationImpl {
  /*
   Publisher: set up event producer push - but receiver must forward to RES entities, not write directly to event log
   for conflict resolution

   consumer: regular replication channel
   */
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      producerFilter: EventEnvelope[Event] => Boolean,
      replicatedEntity: ReplicatedEntity[Command])(implicit system: ActorSystem[_]): ReplicationImpl[Command] = ???
}
