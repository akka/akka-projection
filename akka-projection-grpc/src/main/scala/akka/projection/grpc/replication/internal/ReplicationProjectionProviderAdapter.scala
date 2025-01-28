/*
 * Copyright (C) 2009-2025 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.grpc.replication.internal

import akka.Done
import akka.NotUsed
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.projection.BySlicesSourceProvider
import akka.projection.ProjectionContext
import akka.projection.ProjectionId
import akka.projection.grpc.replication.javadsl.{ ReplicationProjectionProvider => JReplicationProjectionProvider }
import akka.projection.grpc.replication.scaladsl.{ ReplicationProjectionProvider => SReplicationProjectionProvider }
import akka.projection.internal.ScalaToJavaBySlicesSourceProviderAdapter
import akka.projection.scaladsl.{ AtLeastOnceFlowProjection => SAtLeastOnceFlowProjection }
import akka.projection.scaladsl.{ SourceProvider => SSourceProvider }
import akka.stream.scaladsl.{ FlowWithContext => SFlowWithContext }

/**
 * INTERNAL API
 */
@InternalApi
private[akka] object ReplicationProjectionProviderAdapter {
  def toScala(provider: JReplicationProjectionProvider): SReplicationProjectionProvider = {
    (
        projectionId: ProjectionId,
        sourceProvider: SSourceProvider[Offset, EventEnvelope[AnyRef]],
        replicationFlow: SFlowWithContext[EventEnvelope[AnyRef], ProjectionContext, Done, ProjectionContext, NotUsed],
        system: ActorSystem[_]) =>
      val providerWithSlices = sourceProvider match {
        case withSlices: SSourceProvider[Offset, EventEnvelope[AnyRef]] with BySlicesSourceProvider => withSlices
        case noSlices =>
          throw new IllegalArgumentException(
            s"The source provider is required to implement akka.projection.BySlicesSourceProvider but ${noSlices.getClass} does not")
      }

      val sourceProviderAdapter = ScalaToJavaBySlicesSourceProviderAdapter(providerWithSlices)
      val javaProjection =
        provider.create(projectionId, sourceProviderAdapter, replicationFlow.asJava, system)
      javaProjection match {
        case alsoSProjection: SAtLeastOnceFlowProjection[Offset @unchecked, EventEnvelope[AnyRef] @unchecked] =>
          alsoSProjection

        case other =>
          // FIXME can we really expect that projections always implement both?
          throw new IllegalArgumentException(s"Unsupported type of projection ${other.getClass}")
      }
  }
}
