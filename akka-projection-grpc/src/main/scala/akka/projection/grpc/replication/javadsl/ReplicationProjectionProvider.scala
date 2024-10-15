/*
 * Copyright (C) 2009-2024 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.javadsl

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.javadsl.AtLeastOnceFlowProjection
import akka.projection.javadsl.SourceProvider
import akka.stream.javadsl.FlowWithContext

/**
 * Factory for creating the projection where offsets are kept track of for the replication streams
 */
@FunctionalInterface
trait ReplicationProjectionProvider {

  def create(
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, EventEnvelope[AnyRef]],
      replicationFlow: FlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, EventEnvelope[AnyRef]]

}
