/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.javadsl

import java.util.concurrent.CompletionStage

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.annotation.DoNotInherit
import akka.projection.HandlerRecoveryStrategy
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.cassandra.internal.HandlerAdapter
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.Handler
import akka.projection.javadsl.SourceProvider

/**
 * Factories of [[Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 */
@ApiMayChange
object CassandraProjection {

  /**
   * Create a [[Projection]] with at-least-once processing semantics. It stores the offset in Cassandra
   * after the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset some elements may be processed more than once.
   */
  def atLeastOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtLeastOnceCassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      CassandraProjectionImpl.AtLeastOnce(),
      settingsOpt = None,
      new HandlerAdapter(handler))

  /**
   * Create a [[Projection]] with at-most-once processing semantics. It stores the offset in Cassandra
   * before the `handler` has processed the envelope. This means that if the projection is restarted
   * from previously stored offset one envelope may not have been processed.
   */
  def atMostOnce[Offset, Envelope](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      handler: Handler[Envelope]): AtMostOnceCassandraProjection[Envelope] =
    new CassandraProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      CassandraProjectionImpl.AtMostOnce(),
      settingsOpt = None,
      new HandlerAdapter(handler))
}

@DoNotInherit trait CassandraProjection[Envelope] extends Projection[Envelope] {

  override def withSettings(settings: ProjectionSettings): CassandraProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def initializeOffsetTable(systemProvider: ClassicActorSystemProvider): CompletionStage[Done]
}

@DoNotInherit trait AtLeastOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtLeastOnceCassandraProjection[Envelope]

  def withSaveOffset(afterEnvelopes: Int, afterDuration: java.time.Duration): AtLeastOnceCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): AtLeastOnceCassandraProjection[Envelope]
}

@DoNotInherit trait AtMostOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  override def withSettings(settings: ProjectionSettings): AtMostOnceCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: StrictRecoveryStrategy): AtMostOnceCassandraProjection[Envelope]
}
