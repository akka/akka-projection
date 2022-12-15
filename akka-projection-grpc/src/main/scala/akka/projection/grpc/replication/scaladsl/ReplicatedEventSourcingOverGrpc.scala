/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.Persistence
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.PublishedEventImpl
import akka.persistence.typed.internal.ReplicatedPublishedEventMetaData
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.replication.Replica
import akka.projection.grpc.replication.ReplicationSettings
import akka.projection.r2dbc.scaladsl.R2dbcProjection
import akka.stream.scaladsl.FlowWithContext
import akka.util.Timeout
import com.google.protobuf.Descriptors

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class ReplicatedEventSourcingOverGrpc[Command] private (
    val service: PartialFunction[HttpRequest, Future[HttpResponse]],
    val entityTypeKey: EntityTypeKey[Command])

object ReplicatedEventSourcingOverGrpc {

  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicationContext => EventSourcedBehavior[Command, Event, State])(
      implicit system: ActorSystem[_]): ReplicatedEventSourcingOverGrpc[Command] = {
    // FIXME verify we have cluster or fail

    val allReplicaIds = settings.otherReplicas.map(_.replicaId) + settings.selfReplicaId

    // FIXME how do the user wire it up? return the route?
    // set up a publisher
    val eps = EventProducerSource(
      settings.entityTypeKey.name,
      settings.streamId,
      Transformation.identity,
      settings.eventProducerSettings)
    val eventProducerRoute = EventProducer.grpcServiceHandler(eps)

    // sharding for forwarding events
    val sharding = ClusterSharding(system)
    val entity = Entity[Command](settings.entityTypeKey) { entityContext =>
      val replicationId =
        ReplicationId(settings.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
      ReplicatedEventSourcing.externalReplication(replicationId, allReplicaIds)(replicatedBehaviorFactory)
    }
    sharding.init(entity)

    // sharded daemon process for consuming event stream from the other dc:s
    settings.otherReplicas.foreach { replica =>
      startConsumer(
        settings.entityTypeKey.name,
        replica,
        settings.protobufDescriptors,
        sharding.entityRefFor(entity.typeKey, _))
    }

    // FIXME user still has to start the producer, could we opt-out do that as well?
    new ReplicatedEventSourcingOverGrpc[Command](eventProducerRoute, entity.typeKey)
  }

  private def startConsumer[C](
      entityTypeKeyName: String,
      replica: Replica,
      protobufDescriptors: Seq[Descriptors.FileDescriptor],
      entityRefFactory: String => EntityRef[C])(implicit system: ActorSystem[_]): Unit = {
    implicit val timeout: Timeout = 5.seconds // FIXME from config

    val projectionName = s"${entityTypeKeyName}_${replica.replicaId.id}"
    val sliceRanges = Persistence(system).sliceRanges(replica.numberOfConsumers)

    val eventsBySlicesQuery =
      GrpcReadJournal(replica.grpcQuerySettings, replica.grpcClientSettings, protobufDescriptors)

    ShardedDaemonProcess(system).init(projectionName, replica.numberOfConsumers, {
      idx =>
        val sliceRange = sliceRanges(idx)
        val projectionKey =
          s"${eventsBySlicesQuery.streamId}-${sliceRange.min}-${sliceRange.max}"
        val projectionId = ProjectionId.of(projectionName, projectionKey)

        val replicationFlow = FlowWithContext[EventEnvelope[_], ProjectionContext].mapAsync(1) { envelope =>
          val entityRef = entityRefFactory(envelope.persistenceId).asInstanceOf[EntityRef[PublishedEventImpl]]
          entityRef.ask[Done](
            replyTo =>
              PublishedEventImpl(
                PersistenceId.ofUniqueId(envelope.persistenceId),
                envelope.sequenceNr,
                envelope.event,
                envelope.timestamp,
                // FIXME actually provide the right metadata here
                envelope.eventMetadata.map(_.asInstanceOf[ReplicatedPublishedEventMetaData]),
                Some(replyTo)))
        }

        val sourceProvider = EventSourcedProvider.eventsBySlices[AnyRef](
          system,
          eventsBySlicesQuery,
          eventsBySlicesQuery.streamId,
          sliceRange.min,
          sliceRange.max)

        ProjectionBehavior(
          // FIXME support more/arbitrary projection impls?
          R2dbcProjection.atLeastOnceFlow(projectionId, None, sourceProvider, replicationFlow))
    })
  }

}
