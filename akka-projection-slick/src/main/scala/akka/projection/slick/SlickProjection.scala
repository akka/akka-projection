/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.Done
import akka.actor.ClassicActorSystemProvider
import akka.annotation.ApiMayChange
import akka.projection.HandlerRecoveryStrategy.Internal.AtLeastOnceRecoveryStrategy
import akka.projection.HandlerRecoveryStrategy.Internal.ExactlyOnceRecoveryStrategy
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickProjectionImpl
import akka.projection.slick.internal.SlickProjectionImpl.AtLeastOnce
import akka.projection.slick.internal.SlickProjectionImpl.ExactlyOnce
import akka.projection.slick.internal.SlickProjectionImpl.Strategy
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

/**
 * Factories of [[Projection]] where the offset is stored in a relational database table using Slick.
 * The envelope handler can integrate with anything, such as publishing to a message broker, or updating a relational read model.
 */
@ApiMayChange
object SlickProjection {

  /**
   * Create a [[Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   *
   */
  def exactlyOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[Envelope]): ExactlyOnceSlickProjection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.ExactlyOnce(),
      settingsOpt = None,
      handler)

  /**
   * Create a [[Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset some elements may be processed more than once.
   */
  def atLeastOnce[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[Envelope]): AtLeastOnceSlickProjection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.AtLeastOnce(),
      settingsOpt = None,
      handler)

}

trait SlickProjection[Envelope] extends Projection[Envelope] {
  private[slick] def strategy: Strategy

  override def withSettings(settings: ProjectionSettings): SlickProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit systemProvider: ClassicActorSystemProvider): Future[Done]
}

trait AtLeastOnceSlickProjection[Envelope] extends SlickProjection[Envelope] {
  private[slick] def atLeastOnceStrategy: AtLeastOnce = strategy.asInstanceOf[AtLeastOnce]

  override def withSettings(settings: ProjectionSettings): AtLeastOnceSlickProjection[Envelope]

  def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): AtLeastOnceSlickProjection[Envelope]

  /**
   * Java API
   */
  def withSaveOffset(afterEnvelopes: Int, afterDuration: java.time.Duration): AtLeastOnceSlickProjection[Envelope]

  def withAtLeastOnceRecoveryStrategy(
      recoveryStrategy: AtLeastOnceRecoveryStrategy): AtLeastOnceSlickProjection[Envelope]
}

trait ExactlyOnceSlickProjection[Envelope] extends SlickProjection[Envelope] {
  private[slick] def exactlyOnceStrategy: ExactlyOnce = strategy.asInstanceOf[ExactlyOnce]

  override def withSettings(settings: ProjectionSettings): ExactlyOnceSlickProjection[Envelope]

  def withExactlyOnceRecoveryStrategy(
      recoveryStrategy: ExactlyOnceRecoveryStrategy): ExactlyOnceSlickProjection[Envelope]
}

object SlickHandler {

  /** SlickHandler that can be define from a simple function */
  private class SlickHandlerFunction[Envelope](handler: Envelope => DBIO[Done]) extends SlickHandler[Envelope] {
    override def process(envelope: Envelope): DBIO[Done] = handler(envelope)
  }

  def apply[Envelope](handler: Envelope => DBIO[Done]): SlickHandler[Envelope] = new SlickHandlerFunction(handler)
}

/**
 * Implement this interface for the Envelope handler in [[SlickProjection]].
 *
 * It can be stateful, with variables and mutable data structures.
 * It is invoked by the `Projection` machinery one envelope at a time and visibility
 * guarantees between the invocations are handled automatically, i.e. no volatile or
 * other concurrency primitives are needed for managing the state.
 *
 * Error handling strategy for when processing an `Envelope` fails can be defined in the supported
 * [[HandlerRecoveryStrategyHandler]] in settings or passed to the [[akka.projection.Projection]].
 */
@ApiMayChange
trait SlickHandler[Envelope] extends HandlerLifecycle {

  def process(envelope: Envelope): DBIO[Done]

}
