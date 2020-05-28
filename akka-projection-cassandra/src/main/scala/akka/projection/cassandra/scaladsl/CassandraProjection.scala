/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.cassandra.scaladsl

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.HandlerRecoveryStrategy
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.StrictRecoveryStrategy
import akka.projection.cassandra.internal.CassandraProjectionImpl
import akka.projection.cassandra.internal.CassandraProjectionImpl.AtLeastOnce
import akka.projection.cassandra.internal.CassandraProjectionImpl.AtMostOnce
import akka.projection.cassandra.internal.CassandraProjectionImpl.Strategy
import akka.projection.scaladsl.Handler
import akka.projection.scaladsl.SourceProvider

/**
 * Factories of [[Projection]] where the offset is stored in Cassandra. The envelope handler can
 * integrate with anything, such as publishing to a message broker, or updating a read model in Cassandra.
 *
 * The envelope handler function can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
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
      sourceProvider,
      CassandraProjectionImpl.AtLeastOnce(),
      settingsOpt = None,
      handler)

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
      sourceProvider,
      CassandraProjectionImpl.AtMostOnce(),
      settingsOpt = None,
      handler)
}

trait CassandraProjection[Envelope] extends Projection[Envelope] {
  private[cassandra] def strategy: Strategy

  override def withSettings(settings: ProjectionSettings): CassandraProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done]
}

trait AtLeastOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  private[cassandra] def atLeastOnceStrategy: AtLeastOnce = strategy.asInstanceOf[AtLeastOnce]

  override def withSettings(settings: ProjectionSettings): AtLeastOnceCassandraProjection[Envelope]

  def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): AtLeastOnceCassandraProjection[Envelope]
  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): AtLeastOnceCassandraProjection[Envelope]
}

trait AtMostOnceCassandraProjection[Envelope] extends CassandraProjection[Envelope] {
  private[cassandra] def atMostOnceStrategy: AtMostOnce = strategy.asInstanceOf[AtMostOnce]

  override def withSettings(settings: ProjectionSettings): AtMostOnceCassandraProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: StrictRecoveryStrategy): AtMostOnceCassandraProjection[Envelope]
}
