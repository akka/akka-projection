/*
 * Copyright (C) 2009-2022 Lightbend Inc. <https://www.lightbend.com>
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
import akka.projection.grpc.replication.scaladsl.{ ReplicationProjectionProvider => SReplicationProjectionProvider }
import akka.projection.internal.ScalaSourceProviderAdapter
import akka.projection.scaladsl.{ SourceProvider => SSourceProvider }
import akka.stream.scaladsl.{ FlowWithContext => SFlowWithContext }
import akka.projection.scaladsl.{ AtLeastOnceFlowProjection => SAtLeastOnceFlowProjection }

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

/**
 * INTERNAL API
 */
private[akka] object ReplicationProjectionProviderAdapter {
  def toScala(provider: ReplicationProjectionProvider): SReplicationProjectionProvider = {
    (
        projectionId: ProjectionId,
        sourceProvider: SSourceProvider[Offset, EventEnvelope[AnyRef]],
        replicationFlow: SFlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed],
        system: ActorSystem[_]) =>
      val javaProjection =
        provider.create(projectionId, new ScalaSourceProviderAdapter(sourceProvider), replicationFlow.asJava, system)
      javaProjection match {
        case alsoSProjection: SAtLeastOnceFlowProjection[Offset @unchecked, EventEnvelope[AnyRef] @unchecked] =>
          alsoSProjection

        case other =>
          // FIXME can we really expect that projections always implement both?
          throw new IllegalArgumentException(s"Unsupported type of projection ${other.getClass}")
      }
  }
}
