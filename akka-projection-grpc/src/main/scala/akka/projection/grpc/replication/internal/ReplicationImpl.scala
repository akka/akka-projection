/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.Done
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.cluster.ClusterActorRefProvider
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
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
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.Replication
import akka.stream.scaladsl.FlowWithContext
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * INTERNAL API
 */
@InternalApi
private[akka] final class ReplicationImpl[Command] private (
    val eventProducerService: EventProducerSource,
    val createSingleServiceHandler: () => PartialFunction[HttpRequest, Future[HttpResponse]],
    val entityTypeKey: EntityTypeKey[Command],
    val entityRefFactory: String => EntityRef[Command])
    extends Replication[Command] {
  override def toString: String = s"Replication(${eventProducerService.entityType}, ${eventProducerService.entityType})"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ReplicationImpl {

  private val log = LoggerFactory.getLogger(classOf[ReplicationImpl[_]])

  private val filteredEvent = Future.successful(None)

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](settings: ReplicationSettingsImpl[Command])(
      replicatedBehaviorFactory: ReplicationContext => EventSourcedBehavior[Command, Event, State])(
      implicit system: ActorSystem[_]): ReplicationImpl[Command] = {
    require(
      system.classicSystem.asInstanceOf[ExtendedActorSystem].provider.isInstanceOf[ClusterActorRefProvider],
      "Replicated Event Sourcing over gRPC only possible together with Akka cluster (akka.actor.provider = cluster)")

    val allReplicaIds = settings.otherReplicas.map(_.replicaId) + settings.selfReplicaId

    // set up a publisher
    val onlyLocalOriginTransformer = Transformation.empty.registerAsyncEnvelopeOrElseMapper(envelope =>
      envelope.eventMetadata match {
        case Some(meta: ReplicatedEventMetadata) =>
          if (meta.originReplica == settings.selfReplicaId) Future.successful(envelope.eventOption)
          else filteredEvent // Optimization: was replicated to this DC, don't pass the payload across the wire
        case _ =>
          throw new IllegalArgumentException(
            s"Got an event without replication metadata, not supported (pid: ${envelope.persistenceId}, seq_nr: ${envelope.sequenceNr})")
      })
    val eps = EventProducerSource(
      settings.entityTypeKey.name,
      settings.streamId,
      onlyLocalOriginTransformer,
      settings.eventProducerSettings)

    // sharding for hosting the entities and forwarding events
    val replicatedEntity = {

      ReplicatedEntity(settings.selfReplicaId, settings.configureEntity.apply(Entity(settings.entityTypeKey) {
        entityContext =>
          val replicationId =
            ReplicationId(entityContext.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
          ReplicatedEventSourcing.externalReplication(replicationId, allReplicaIds)(replicatedBehaviorFactory)
      }))
    }

    val sharding = ClusterSharding(system)
    sharding.init(replicatedEntity.entity)

    // sharded daemon process for consuming event stream from the other dc:s
    val entityRefFactory: String => EntityRef[Command] = sharding.entityRefFor(replicatedEntity.entity.typeKey, _)
    settings.otherReplicas.foreach(startConsumer(_, settings, entityRefFactory))

    new ReplicationImpl[Command](
      eventProducerService = eps,
      createSingleServiceHandler = () =>
        EventProducer.grpcServiceHandler(Set(eps), settings.eventProducerInterceptor, includeMetadata = true),
      entityTypeKey = replicatedEntity.entity.typeKey,
      entityRefFactory = entityRefFactory)
  }

  private def startConsumer[C](
      remoteReplica: Replica,
      settings: ReplicationSettingsImpl[C],
      entityRefFactory: String => EntityRef[C])(implicit system: ActorSystem[_]): Unit = {
    implicit val timeout: Timeout = settings.entityEventReplicationTimeout
    implicit val ec: ExecutionContext = system.executionContext

    val projectionName =
      s"RES_${settings.entityTypeKey.name}_${settings.selfReplicaId.id}_${remoteReplica.replicaId.id}"
    require(
      projectionName.size < 255,
      s"The generated projection name for replica [${remoteReplica.replicaId.id}]: '$projectionName' is too long to fit " +
      "in the database column, must be at most 255 characters. See if you can shorten replica or entity type names.")
    val sliceRanges = Persistence(system).sliceRanges(remoteReplica.numberOfConsumers)

    val grpcQuerySettings = {
      val s = GrpcQuerySettings(settings.streamId)
      remoteReplica.additionalQueryRequestMetadata.fold(s)(s.withAdditionalRequestMetadata)
    }
    val eventsBySlicesQuery = GrpcReadJournal(grpcQuerySettings, remoteReplica.grpcClientSettings, Nil)

    val shardedDaemonProcessSettings = {
      val default = ShardedDaemonProcessSettings(system)
      remoteReplica.consumersOnClusterRole match {
        case None       => default
        case Some(role) => default.withRole(role)
      }
    }
    ShardedDaemonProcess(system).init(projectionName, remoteReplica.numberOfConsumers, { idx =>
      val sliceRange = sliceRanges(idx)
      val projectionKey = s"${sliceRange.min}-${sliceRange.max}"
      val projectionId = ProjectionId(projectionName, projectionKey)

      val replicationFlow: FlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed] =
        FlowWithContext[EventEnvelope[AnyRef], ProjectionContext]
          .via(new ParallelUpdatesFlow[AnyRef](16)({
            envelope =>
              envelope.eventMetadata match {
                case Some(replicatedEventMetadata: ReplicatedEventMetadata)
                    if replicatedEventMetadata.originReplica == remoteReplica.replicaId =>
                  val replicationId = ReplicationId.fromString(envelope.persistenceId)
                  val destinationReplicaId = replicationId.withReplica(settings.selfReplicaId)
                  val entityRef =
                    entityRefFactory(destinationReplicaId.entityId).asInstanceOf[EntityRef[PublishedEvent]]
                  if (log.isDebugEnabled) {
                    log.debugN(
                      "[{}], Forwarding event originating on dc [{}] to [{}] (version: [{}]): [{}]",
                      system.name,
                      replicatedEventMetadata.originReplica,
                      destinationReplicaId.persistenceId.id,
                      replicatedEventMetadata.version,
                      envelope.event)
                  }
                  val askResult = entityRef.ask[Done](replyTo =>
                    PublishedEventImpl(
                      replicationId.persistenceId,
                      replicatedEventMetadata.originSequenceNr,
                      envelope.event,
                      envelope.timestamp,
                      Some(new ReplicatedPublishedEventMetaData(
                        replicatedEventMetadata.originReplica,
                        replicatedEventMetadata.version)),
                      Some(replyTo)))
                  askResult.failed.foreach(error =>
                    log.warn(s"Failing replication stream from [${remoteReplica.replicaId.id}]", error))
                  askResult
                case Some(NotUsed) =>
                  // Events not originating on sending side already are filtered/have no payload and end up here
                  log.debugN("[{}], Ignoring filtered event from [{}]", system.name, remoteReplica.replicaId)
                  Future.successful(Done)
                case other => throw new IllegalArgumentException(s"Unknown type or missing metadata: [$other]")
              }
          }))
          .map(_ => Done)

      val sourceProvider = EventSourcedProvider.eventsBySlices[AnyRef](
        system,
        eventsBySlicesQuery,
        eventsBySlicesQuery.streamId,
        sliceRange.min,
        sliceRange.max)
      ProjectionBehavior(settings.projectionProvider(projectionId, sourceProvider, replicationFlow, system))
    }, shardedDaemonProcessSettings, None) // FIXME why doesn't Some(ProjectionBehavior.Stop) work
  }

}
