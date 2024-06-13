/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.scaladsl

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.scaladsl.AtLeastOnceFlowProjection
import akka.projection.scaladsl.SourceProvider
import akka.stream.scaladsl.FlowWithContext

/**
 * Factory/function for creating the projection where offsets are kept track of for the replication streams
 */
trait ReplicationProjectionProvider {
  def apply(
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, EventEnvelope[AnyRef]],
      replicationFlow: FlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed],
      system: ActorSystem[_]): AtLeastOnceFlowProjection[Offset, EventEnvelope[AnyRef]]
}
