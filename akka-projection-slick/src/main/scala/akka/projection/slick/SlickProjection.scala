/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.slick

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.ApiMayChange
import akka.projection.HandlerRecoveryStrategy
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.ProjectionSettings
import akka.projection.internal.NoopStatusObserver
import akka.projection.scaladsl.HandlerLifecycle
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickProjectionImpl
import akka.projection.slick.internal.SlickProjectionImpl.AtLeastOnce
import akka.projection.slick.internal.SlickProjectionImpl.ExactlyOnce
import akka.projection.slick.internal.SlickProjectionImpl.OffsetStrategy
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
      settingsOpt = None,
      SlickProjectionImpl.ExactlyOnce(),
      SlickProjectionImpl.SingleHandlerStrategy(handler),
      NoopStatusObserver)

  /**
   * Create a [[Projection]] with at-least-once processing semantics.
   *
   * It stores the offset in a relational database table using Slick after the `handler` has processed the envelope.
   * This means that if the projection is restarted from previously stored offset some elements may be processed more
   * than once.
   *
   * The offset is stored after a time window, or limited by a number of envelopes, whatever happens first.
   * This window can be defined with [[AtLeastOnceSlickProjection.withSaveOffset]] of the returned
   * `AtLeastOnceSlickProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.at-least-once`.
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
      settingsOpt = None,
      SlickProjectionImpl.AtLeastOnce(),
      SlickProjectionImpl.SingleHandlerStrategy(handler),
      NoopStatusObserver)

  /**
   * Create a [[Projection]] that groups envelopes and calls the `handler` with a group of `Envelopes`.
   * The envelopes are grouped within a time window, or limited by a number of envelopes,
   * whatever happens first. This window can be defined with [[GroupedSlickProjection.withGroup]] of
   * the returned `GroupedSlickProjection`. The default settings for the window is defined in configuration
   * section `akka.projection.grouped`.
   *
   * It stores the offset in a relational database table using Slick in the same transaction
   * as the DBIO returned from the `handler`.
   */
  def groupedWithin[Offset, Envelope, P <: JdbcProfile: ClassTag](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      databaseConfig: DatabaseConfig[P],
      handler: SlickHandler[immutable.Seq[Envelope]]): GroupedSlickProjection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      settingsOpt = None,
      SlickProjectionImpl.ExactlyOnce(),
      SlickProjectionImpl.GroupedHandlerStrategy(handler),
      NoopStatusObserver)

}

trait SlickProjection[Envelope] extends Projection[Envelope] {
  private[slick] def offsetStrategy: OffsetStrategy

  override def withSettings(settings: ProjectionSettings): SlickProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): Future[Done]
}

trait AtLeastOnceSlickProjection[Envelope] extends SlickProjection[Envelope] {
  private[slick] def atLeastOnceStrategy: AtLeastOnce = offsetStrategy.asInstanceOf[AtLeastOnce]

  override def withSettings(settings: ProjectionSettings): AtLeastOnceSlickProjection[Envelope]

  def withSaveOffset(afterEnvelopes: Int, afterDuration: FiniteDuration): AtLeastOnceSlickProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): AtLeastOnceSlickProjection[Envelope]
}

trait GroupedSlickProjection[Envelope] extends SlickProjection[Envelope] {

  override def withSettings(settings: ProjectionSettings): GroupedSlickProjection[Envelope]

  def withGroup(groupAfterEnvelopes: Int, groupAfterDuration: FiniteDuration): GroupedSlickProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): GroupedSlickProjection[Envelope]
}

trait ExactlyOnceSlickProjection[Envelope] extends SlickProjection[Envelope] {
  private[slick] def exactlyOnceStrategy: ExactlyOnce = offsetStrategy.asInstanceOf[ExactlyOnce]

  override def withSettings(settings: ProjectionSettings): ExactlyOnceSlickProjection[Envelope]

  def withRecoveryStrategy(recoveryStrategy: HandlerRecoveryStrategy): ExactlyOnceSlickProjection[Envelope]
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
 * Supported error handling strategies for when processing an `Envelope` fails can be
 * defined in configuration or using the `withRecoveryStrategy` method of a `Projection`
 * implementation.
 */
@ApiMayChange
trait SlickHandler[Envelope] extends HandlerLifecycle {

  def process(envelope: Envelope): DBIO[Done]

}
