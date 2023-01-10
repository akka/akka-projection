/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.cluster.sharding.typed.scaladsl.EntityRef
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.HttpResponse
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplicationContext
import akka.projection.grpc.producer.scaladsl.EventProducer.EventProducerSource
import akka.projection.grpc.replication.internal.ReplicationImpl
import akka.projection.grpc.replication.internal.ReplicationSettingsImpl

import scala.concurrent.Future

/**
 * Created using [[Replication#grpcRepliation]], which starts sharding with the entity and
 * replication stream consumers but not the replication endpoint needed to publish events to other replication places.
 *
 * @tparam Command The type of commands the Replicated Event Sourced Entity accepts
 *
 * Not for user extension
 */
@ApiMayChange
@DoNotInherit
trait Replication[Command] {

  /**
   * If combining multiple entity types replicated, or combining with direct usage of
   * Akka Projection gRPC you will have to use the EventProducerService of each of them
   * in a set passed to EventProducer.grpcServiceHandler to create a single gRPC endpoint
   */
  def eventProducerService: EventProducerSource

  /**
   * If only replicating one Replicated Event Sourced Entity and not using
   * Akka Projection gRPC this endpoint factory can be used to get a partial function
   * that can be served/bound with an Akka HTTP/2 server
   */
  def createSingleServiceHandler: () => PartialFunction[HttpRequest, Future[HttpResponse]]

  /**
   * Entity type key for looking up the entities
   */
  def entityTypeKey: EntityTypeKey[Command]

  /**
   * Shortcut for creating EntityRefs for the sharded Replicated Event Sourced entities for
   * sending commands.
   */
  def entityRefFactory: String => EntityRef[Command]
}

@ApiMayChange
object Replication {

  /**
   * Called to bootstrap the entity on each cluster node in each of the replicas.
   *
   * Important: Note that this does not publish the endpoint, additional steps are needed!
   */
  def grpcReplication[Command, Event, State](settings: ReplicationSettings[Command])(
      replicatedBehaviorFactory: ReplicationContext => EventSourcedBehavior[Command, Event, State])(
      implicit system: ActorSystem[_]): Replication[Command] =
    ReplicationImpl.grpcReplication[Command, Event, State](settings.asInstanceOf[ReplicationSettingsImpl[Command]])(
      replicatedBehaviorFactory)(system)

}
