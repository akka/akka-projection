package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.projection.grpc.replication.internal.PushReplicationImpl

/**
 * An active producer for replicated event sourcing that can be started on the producer to connect a service to replicate
 * events both ways but with connections only going in one direction, for example to run a projection piercing firewalls
 * or NAT.
 */
object PushReplication {

  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      producerFilter: EventEnvelope[Event] => Boolean)(
      replicatedBehaviorFactory: ReplicatedBehaviors[Command, Event, State] => Behavior[Command])(
      implicit system: ActorSystem[_]): Replication[Command] = {

    val replicatedEntity =
      ReplicatedEntity(
        settings.selfReplicaId,
        settings.configureEntity.apply(Entity(settings.entityTypeKey) { entityContext =>
          val replicationId =
            ReplicationId(entityContext.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
          replicatedBehaviorFactory { factory =>
            ReplicatedEventSourcing.externalReplication(
              replicationId,
              settings.otherReplicas.map(_.replicaId) + settings.selfReplicaId)(factory)
          }
        }))

    PushReplicationImpl.grpcReplication[Command, Event, State](settings, producerFilter, replicatedEntity)
  }
}
