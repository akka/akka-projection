/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.ApiMayChange
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
import akka.projection.grpc.replication.Replica
import akka.projection.grpc.replication.ReplicationSettings
import akka.stream.scaladsl.FlowWithContext
import akka.util.Timeout
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/**
 * Created using [[Replication#grpcRepliation]], which starts sharding with the entity and
 * replication stream consumers but not the replication endpoint needed to publish events to other replication places.
 *
 * @param eventProducerService If combining multiple entity types replicated, or combining with direct usage of
 *                             Akka Projection gRPC you will have to use the EventProducerService of each of them
 *                             in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
 * @param createSingleServiceHandler If only replicating one Replicated Event Sourced Entity and not using
 *                                   Akka Projection gRPC this endpoint factory can be used to get a partial function
 *                                   that can be served/bound with an Akka HTTP server
 * @param entityTypeKey Entity type key for looking up the
 * @param entityRefFactory Shortcut for creating EntityRefs for the sharded Replicated Event Sourced entities for
 *                         sending commands.
 * @tparam Command The type of commands the Replicated Event Sourced Entity accepts
 */
@ApiMayChange
final class Replication[Command] private (
    val eventProducerService: EventProducerSource,
    val createSingleServiceHandler: () => PartialFunction[HttpRequest, Future[HttpResponse]],
    val entityTypeKey: EntityTypeKey[Command],
    val entityRefFactory: String => EntityRef[Command])

@ApiMayChange
object Replication {

  private val log = LoggerFactory.getLogger(classOf[Replication[_]])

  private val filteredEvent = Future.successful(None)

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicationContext => EventSourcedBehavior[Command, Event, State])(
      implicit system: ActorSystem[_]): Replication[Command] = {
    require(
      system.settings.config.getString("akka.actor.provider") == "cluster",
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
    val replicatedEntity =
      ReplicatedEntity(settings.selfReplicaId, Entity(settings.entityTypeKey) { entityContext =>
        val replicationId =
          ReplicationId(entityContext.entityTypeKey.name, entityContext.entityId, settings.selfReplicaId)
        ReplicatedEventSourcing.externalReplication(replicationId, allReplicaIds)(replicatedBehaviorFactory)
      })

    val sharding = ClusterSharding(system)
    sharding.init(replicatedEntity.entity)

    // sharded daemon process for consuming event stream from the other dc:s
    val entityRefFactory: String => EntityRef[Command] = sharding.entityRefFor(replicatedEntity.entity.typeKey, _)
    settings.otherReplicas.foreach(startConsumer(_, settings, entityRefFactory))

    new Replication[Command](
      eventProducerService = eps,
      createSingleServiceHandler = () =>
        EventProducer.grpcServiceHandler(Set(eps), settings.producerInterceptor, includeMetadata = true),
      entityTypeKey = replicatedEntity.entity.typeKey,
      entityRefFactory = entityRefFactory)
  }

  private def startConsumer[C](
      remoteReplica: Replica,
      settings: ReplicationSettings[C],
      entityRefFactory: String => EntityRef[C])(implicit system: ActorSystem[_]): Unit = {
    implicit val timeout: Timeout = settings.entityEventReplicationTimeout
    implicit val ec: ExecutionContext = system.executionContext

    val projectionName = s"${settings.entityTypeKey.name}_${remoteReplica.replicaId.id}"
    val sliceRanges = Persistence(system).sliceRanges(remoteReplica.numberOfConsumers)

    val grpcQuerySettings = {
      val s = GrpcQuerySettings(settings.streamId)
      remoteReplica.additionalRequestMetadata.fold(s)(s.withAdditionalRequestMetadata)
    }
    val eventsBySlicesQuery = GrpcReadJournal(grpcQuerySettings, remoteReplica.grpcClientSettings, Nil)

    ShardedDaemonProcess(system).init(projectionName, remoteReplica.numberOfConsumers, { idx =>
      val sliceRange = sliceRanges(idx)
      val projectionKey =
        s"${eventsBySlicesQuery.streamId}-${remoteReplica.replicaId.id}-${sliceRange.min}-${sliceRange.max}"
      val projectionId = ProjectionId.of(projectionName, projectionKey)

      // FIXME allow parallel processing of N events to different entities here
      //       (we can't just increase parallelism as individual entities must have at most one element in flight
      //        or events could be missed because of persisting out of order)
      val replicationFlow: FlowWithContext[EventEnvelope[_], ProjectionContext, Done, ProjectionContext, NotUsed] =
        FlowWithContext[EventEnvelope[_], ProjectionContext]
          .mapAsync(1) {
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
                case Some(NotUsed) =>
                  // Events not originating on sending side already are filtered/have no payload and end up here
                  log.debugN("[{}], Ignoring filtered event from [{}]", system.name, remoteReplica.replicaId)
                  Future.successful(Done)
                case other => throw new IllegalArgumentException(s"Unknown type or missing metadata: [$other]")
              }
          }

      val sourceProvider = EventSourcedProvider.eventsBySlices[AnyRef](
        system,
        eventsBySlicesQuery,
        eventsBySlicesQuery.streamId,
        sliceRange.min,
        sliceRange.max)
      ProjectionBehavior(settings.projectionProvider(projectionId, sourceProvider, replicationFlow, system))
    })
  }

}
