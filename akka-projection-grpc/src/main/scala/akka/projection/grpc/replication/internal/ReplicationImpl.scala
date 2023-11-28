/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.Done
import akka.NotUsed
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.LoggerOps
import akka.annotation.InternalApi
import akka.cluster.ClusterActorRefProvider
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.ReplicatedEntity
import akka.cluster.sharding.typed.ShardedDaemonProcessContext
import akka.cluster.sharding.typed.ShardedDaemonProcessSettings
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
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
import akka.projection.ProjectionBehavior
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.grpc.consumer.ConsumerFilter
import akka.projection.grpc.consumer.GrpcQuerySettings
import akka.projection.grpc.consumer.scaladsl.GrpcReadJournal
import akka.projection.grpc.internal.EventPusherConsumerServiceImpl
import akka.projection.grpc.internal.ProtoAnySerialization
import akka.projection.grpc.internal.proto.EventConsumerServicePowerApiHandler
import akka.projection.grpc.producer.scaladsl.EventProducer
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.producer.scaladsl.EventProducer.Transformation
import akka.projection.grpc.producer.scaladsl.EventProducerPush
import akka.projection.grpc.replication.scaladsl.Replica
import akka.projection.grpc.replication.scaladsl.Replication
import akka.projection.grpc.replication.scaladsl.Replication.EdgeReplication
import akka.projection.grpc.replication.scaladsl.ReplicationSettings
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
  override def toString: String = s"Replication(${eventProducerService.entityType}, ${eventProducerService.streamId})"
}

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ReplicationImpl {

  private val log = LoggerFactory.getLogger(classOf[ReplicationImpl[_]])

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](
      settings: ReplicationSettings[Command],
      replicatedEntity: ReplicatedEntity[Command])(implicit system: ActorSystem[_]): ReplicationImpl[Command] = {
    require(
      system.classicSystem.asInstanceOf[ExtendedActorSystem].provider.isInstanceOf[ClusterActorRefProvider],
      "Replicated Event Sourcing over gRPC only possible together with Akka cluster (akka.actor.provider = cluster)")

    if (settings.initialConsumerFilter.nonEmpty) {
      ConsumerFilter(system).ref ! ConsumerFilter.UpdateFilter(settings.streamId, settings.initialConsumerFilter)
    }

    // set up a publisher
    val eps = EventProducerSource(
      settings.entityTypeKey.name,
      settings.streamId,
      Transformation.identity,
      settings.eventProducerSettings,
      settings.producerFilter)
      .withReplicatedEventOriginFilter(new EventOriginFilter(settings.selfReplicaId))

    val sharding = ClusterSharding(system)
    sharding.init(replicatedEntity.entity)

    // sharded daemon process for consuming event stream from the other dc:s
    val entityRefFactory: String => EntityRef[Command] = sharding.entityRefFor(replicatedEntity.entity.typeKey, _)
    settings.otherReplicas.foreach(startConsumer(_, settings, entityRefFactory))

    new ReplicationImpl[Command](eventProducerService = eps, createSingleServiceHandler = () => {
      val handler = EventProducer.grpcServiceHandler(Set(eps), settings.eventProducerInterceptor)
      if (settings.acceptEdgeReplication) {
        // Fold in edge push gRPC consumer service if enabled
        log.info("Edge replication enabled for Replicated Entity [{}]", settings.entityTypeKey.name)
        val pushConsumer = EventConsumerServicePowerApiHandler.partial(
          EventPusherConsumerServiceImpl.applyForRES(
            Set(settings),
            // FIXME prefer depends on caller?
            ProtoAnySerialization.Prefer.Scala))
        handler.orElse(pushConsumer)
      } else handler

    }, entityTypeKey = replicatedEntity.entity.typeKey, entityRefFactory = entityRefFactory)
  }

  private def startConsumer[C](
      remoteReplica: Replica,
      settings: ReplicationSettings[C],
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
    val eventsBySlicesQuery = GrpcReadJournal(
      grpcQuerySettings,
      remoteReplica.grpcClientSettings,
      Nil,
      ProtoAnySerialization.Prefer.Scala,
      Some(settings))
    log.infoN(
      "Starting {} projection streams{} consuming events for Replicated Entity [{}] from [{}] (at {}:{})",
      remoteReplica.numberOfConsumers,
      remoteReplica.consumersOnClusterRole.fold("")(role => s" on nodes with cluster role $role"),
      settings.entityTypeKey.name,
      remoteReplica.replicaId.id,
      remoteReplica.grpcClientSettings.serviceName,
      remoteReplica.grpcClientSettings.defaultPort)

    val shardedDaemonProcessSettings = {
      import scala.concurrent.duration._
      val default = ShardedDaemonProcessSettings(system)
      val shardingSettings = default.shardingSettings.getOrElse(ClusterShardingSettings(system))
      // shorter handoff timeout because in some restart backoff it may take a while for the projections to
      // terminate and we don't want to delay sharding rebalance for too long. 10 seconds actually
      // means that it will wait 5 seconds before stopping them hard (5 seconds is reduced in sharding).
      val handOffTimeout = shardingSettings.tuningParameters.handOffTimeout.min(10.seconds)
      val defaultWithShardingSettings = default.withShardingSettings(
        shardingSettings.withTuningParameters(shardingSettings.tuningParameters.withHandOffTimeout(handOffTimeout)))
      remoteReplica.consumersOnClusterRole match {
        case None       => defaultWithShardingSettings
        case Some(role) => defaultWithShardingSettings.withRole(role)
      }
    }
    ShardedDaemonProcess(system).init(projectionName, remoteReplica.numberOfConsumers, {
      idx =>
        val sliceRange = sliceRanges(idx)
        val projectionKey = s"${sliceRange.min}-${sliceRange.max}"
        val projectionId = ProjectionId(projectionName, projectionKey)

        val replicationFlow
            : FlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed] =
          FlowWithContext[EventEnvelope[AnyRef], ProjectionContext]
            .mapAsyncPartitioned(parallelism = settings.parallelUpdates, perPartition = 1)(envelope =>
              envelope.persistenceId) {
              case (envelope, _) =>
                if (!envelope.filtered) {
                  envelope.eventMetadata match {
                    case Some(replicatedEventMetadata: ReplicatedEventMetadata)
                        if replicatedEventMetadata.originReplica == settings.selfReplicaId =>
                      // skipping events originating from self replica (break cycle)
                      if (log.isTraceEnabled)
                        log.traceN(
                          "[{}] ignoring event from replica [{}] with self origin (pid [{}], seq_nr [{}])",
                          projectionKey,
                          remoteReplica.replicaId,
                          envelope.persistenceId,
                          envelope.sequenceNr)
                      Future.successful(Done)

                    case Some(replicatedEventMetadata: ReplicatedEventMetadata) =>
                      val replicationId = ReplicationId.fromString(envelope.persistenceId)
                      val destinationReplicaId = replicationId.withReplica(settings.selfReplicaId)
                      val entityRef =
                        entityRefFactory(destinationReplicaId.entityId).asInstanceOf[EntityRef[PublishedEvent]]
                      if (log.isTraceEnabled) {
                        log.traceN(
                          "[{}] forwarding event originating on dc [{}] to [{}] (origin seq_nr [{}]): [{}]",
                          projectionKey,
                          replicatedEventMetadata.originReplica,
                          destinationReplicaId.persistenceId.id,
                          envelope.sequenceNr,
                          replicatedEventMetadata.version)
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
                        log.warn(
                          s"Failing replication stream [$projectionName/$projectionKey] from [${remoteReplica.replicaId.id}], event pid [${envelope.persistenceId}], seq_nr [${envelope.sequenceNr}]",
                          error))
                      askResult

                    case unexpected =>
                      throw new IllegalArgumentException(
                        s"Got unexpected type of event envelope metadata: ${unexpected.getClass} (pid [${envelope.persistenceId}], seq_nr [${envelope.sequenceNr}]" +
                        ", is the remote entity really a Replicated Event Sourced Entity?")
                  }
                } else {
                  // Events not originating on sending side already are filtered/have no payload and end up here
                  if (log.isTraceEnabled)
                    log.traceN(
                      "[{}] ignoring filtered event from replica [{}] (pid [{}], seq_nr [{}])",
                      projectionKey,
                      remoteReplica.replicaId,
                      envelope.persistenceId,
                      envelope.sequenceNr)
                  Future.successful(Done)
                }
            }
            .map(_ => Done)

        val sourceProvider = EventSourcedProvider.eventsBySlices[AnyRef](
          system,
          eventsBySlicesQuery,
          eventsBySlicesQuery.streamId,
          sliceRange.min,
          sliceRange.max)
        ProjectionBehavior(settings.projectionProvider(projectionId, sourceProvider, replicationFlow, system))
    }, shardedDaemonProcessSettings, Some(ProjectionBehavior.Stop))
  }

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   * Does not need to publish any endpoint, both consuming and producing events are
   * active connections from the edge.
   */
  def grpcEdgeReplication[Command](settings: ReplicationSettings[Command], replicatedEntity: ReplicatedEntity[Command])(
      implicit system: ActorSystem[_]): EdgeReplication[Command] = {

    val sharding = ClusterSharding(system)
    sharding.init(replicatedEntity.entity)

    // sharded daemon process for consuming event stream from the other dc:s
    val shardingEntityRefFactory: String => EntityRef[Command] =
      sharding.entityRefFor(replicatedEntity.entity.typeKey, _)

    settings.otherReplicas.foreach { remoteReplica =>
      log.infoN(
        "Starting replication of [{}] to [{}:{}]",
        settings.entityTypeKey.name,
        remoteReplica.grpcClientSettings.serviceName,
        remoteReplica.grpcClientSettings.defaultPort)
      startProducerAndConsumer(remoteReplica, settings, shardingEntityRefFactory)
    }

    new EdgeReplication[Command] {
      override def entityTypeKey: EntityTypeKey[Command] = settings.entityTypeKey
      override def entityRefFactory: String => EntityRef[Command] = shardingEntityRefFactory
    }
  }

  private def startProducerAndConsumer[Command](
      remoteReplica: Replica,
      settings: ReplicationSettings[Command],
      shardingEntityRefFactory: String => EntityRef[Command])(implicit system: ActorSystem[_]): Unit = {
    val projectionName =
      s"RES_${settings.entityTypeKey.name}_${settings.selfReplicaId.id}__${remoteReplica.replicaId.id}"
    require(
      projectionName.size < 255,
      s"The generated projection name for replication: '$projectionName' is too long to fit " +
      "in the database column, must be at most 255 characters. See if you can shorten replica or entity type names.")
    val sliceRanges = Persistence(system).sliceRanges(remoteReplica.numberOfConsumers)

    /* val grpcQuerySettings = {
      val s = GrpcQuerySettings(settings.streamId)
      // FIXME additional request metadata for auth and such
      remoteReplica.additionalQueryRequestMetadata.fold(s)(s.withAdditionalRequestMetadata)
      s
    } */
    // FIXME do we need to know ingress replica id or is just being able to connect (host/port) enough?

    val shardedDaemonProcessSettings = {
      import scala.concurrent.duration._
      val default = ShardedDaemonProcessSettings(system)
      val shardingSettings = default.shardingSettings.getOrElse(ClusterShardingSettings(system))
      // shorter handoff timeout because in some restart backoff it may take a while for the projections to
      // terminate and we don't want to delay sharding rebalance for too long. 10 seconds actually
      // means that it will wait 5 seconds before stopping them hard (5 seconds is reduced in sharding).
      val handOffTimeout = shardingSettings.tuningParameters.handOffTimeout.min(10.seconds)
      default.withShardingSettings(
        shardingSettings.withTuningParameters(shardingSettings.tuningParameters.withHandOffTimeout(handOffTimeout)))
    }

    startConsumer(remoteReplica, settings, shardingEntityRefFactory)

    // start event pushing
    val eps = EventProducerSource(
      settings.entityTypeKey.name,
      settings.streamId,
      Transformation.identity,
      settings.eventProducerSettings)
    val epp = EventProducerPush(settings.entityTypeKey.name, eps, remoteReplica.grpcClientSettings)

    ShardedDaemonProcess(system).initWithContext[ProjectionBehavior.Command](
      s"${settings.selfReplicaId.id}EventProducer",
      // FIXME separate setting for number of producers?
      remoteReplica.numberOfConsumers, { context: ShardedDaemonProcessContext =>
        val sliceRange = sliceRanges(context.processNumber)
        val projectionKey = s"${sliceRange.min}-${sliceRange.max}"
        val projectionId = ProjectionId(projectionName, projectionKey)

        val sourceProvider = EventSourcedProvider
          .eventsBySlices[AnyRef](system, eps.settings.queryPluginId, eps.entityType, sliceRange.min, sliceRange.max)
        val projection = settings.projectionProvider(
          projectionId,
          sourceProvider,
          epp
            .handler()
            .asInstanceOf[FlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed]],
          system)

        ProjectionBehavior(projection)
      },
      shardedDaemonProcessSettings,
      ProjectionBehavior.Stop)

  }

}
