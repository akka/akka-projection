/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.Done
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.Persistence
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.typed.PublishedEvent
import akka.persistence.typed.ReplicaId
import akka.persistence.typed.ReplicationId
import akka.persistence.typed.internal.PublishedEventImpl
import akka.persistence.typed.internal.ReplicatedEventMetadata
import akka.persistence.typed.internal.ReplicatedPublishedEventMetaData
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicatedEventSourcing
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
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

final class ReplicatedEventSourcingOverGrpc[Command] private (
    val service: PartialFunction[HttpRequest, Future[HttpResponse]],
    val entityTypeKey: EntityTypeKey[Command])

object ReplicatedEventSourcingOverGrpc {

  private val log = LoggerFactory.getLogger(classOf[ReplicatedEventSourcingOverGrpc[_]])

  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicationContext => EventSourcedBehavior[Command, Event, State])(
      implicit system: ActorSystem[_]): ReplicatedEventSourcingOverGrpc[Command] = {
    // FIXME verify we have cluster or fail

    val allReplicaIds = settings.otherReplicas.map(_.replicaId) + settings.selfReplicaId

    // set up a publisher

    // FIXME: only pass events with local origin from the producer, not events replicated here in the first place,
    //        not currently possible because no insight into envelope in transformation
    val eps = EventProducerSource(
      settings.entityTypeKey.name,
      settings.streamId,
      Transformation.identity,
      settings.eventProducerSettings)

    val eventProducerRoute = EventProducer.grpcServiceHandler(Set(eps), None, includeMetadata = true)

    // sharding for hosting the entities and forwarding events
    val replicatedEntity =
      ReplicatedEntity(settings.selfReplicaId, Entity(settings.entityTypeKey) { entityContext =>
        val replicationId =
          ReplicationId(entityContext.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
        ReplicatedEventSourcing.externalReplication(replicationId, allReplicaIds)(replicatedBehaviorFactory)
      })

    val sharding = ClusterSharding(system)
    sharding.init(replicatedEntity.entity)

    // sharded daemon process for consuming event stream from the other dc:s
    settings.otherReplicas.foreach { otherReplica =>
      startConsumer(
        settings.entityTypeKey.name,
        settings.selfReplicaId,
        otherReplica,
        settings.protobufDescriptors,
        sharding.entityRefFor(replicatedEntity.entity.typeKey, _))
    }

    // FIXME user still has to start the producer, could we opt-out do that as well?
    new ReplicatedEventSourcingOverGrpc[Command](eventProducerRoute, replicatedEntity.entity.typeKey)
  }

  private def startConsumer[C](
      entityTypeKeyName: String,
      selfReplicaId: ReplicaId,
      remoteReplica: Replica,
      protobufDescriptors: Seq[Descriptors.FileDescriptor],
      entityRefFactory: String => EntityRef[C])(implicit system: ActorSystem[_]): Unit = {
    implicit val timeout: Timeout = 5.seconds // FIXME from config
    implicit val ec: ExecutionContext = system.executionContext

    val projectionName = s"${entityTypeKeyName}_${remoteReplica.replicaId.id}"
    val sliceRanges = Persistence(system).sliceRanges(remoteReplica.numberOfConsumers)

    val eventsBySlicesQuery =
      GrpcReadJournal(remoteReplica.grpcQuerySettings, remoteReplica.grpcClientSettings, protobufDescriptors)

    ShardedDaemonProcess(system).init(projectionName, remoteReplica.numberOfConsumers, { idx =>
      val sliceRange = sliceRanges(idx)
      val projectionKey =
        s"${eventsBySlicesQuery.streamId}-${remoteReplica.replicaId.id}-${sliceRange.min}-${sliceRange.max}"
      val projectionId = ProjectionId.of(projectionName, projectionKey)

      val replicationFlow = FlowWithContext[EventEnvelope[_], ProjectionContext].mapAsync(1) {
        envelope =>

          val replicatedEventMetadata = envelope.eventMetadata match {
            case Some(rm: ReplicatedEventMetadata) => rm
            case other                             => throw new IllegalArgumentException(s"Unknown type or missing metadata: [$other]")
          }
          if (replicatedEventMetadata.originReplica == remoteReplica.replicaId) {
            val replicationId = ReplicationId.fromString(envelope.persistenceId)
            val destinationReplicaId = replicationId.withReplica(selfReplicaId)
            val entityRef = entityRefFactory(destinationReplicaId.entityId).asInstanceOf[EntityRef[PublishedEvent]]
            log.info(
              "[{}], Forwarding event originating on dc [{}] to [{}] (version: [{}]): [{}]",
              system.name,
              replicatedEventMetadata.originReplica,
              destinationReplicaId.persistenceId.id,
              replicatedEventMetadata.version,
              envelope.event)
            val askResult = entityRef.ask[Done](
              replyTo =>
                PublishedEventImpl(
                  replicationId.persistenceId,
                  replicatedEventMetadata.originSequenceNr,
                  envelope.event,
                  envelope.timestamp,
                  Some(
                    new ReplicatedPublishedEventMetaData(
                      replicatedEventMetadata.originReplica,
                      replicatedEventMetadata.version)),
                  Some(replyTo)))
            askResult.failed.foreach(error =>
              log.warn(s"Failing replication stream from [${remoteReplica.replicaId.id}]", error))

            askResult
          } else {
            // FIXME filter on the sending side
            log.debug(
              "[{}], Ignoring event no originating on [{}] replica than ({}): [{}]",
              system.name,
              remoteReplica.replicaId,
              replicatedEventMetadata.originReplica,
              envelope.event)
            Future.successful(Done)
          }
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
