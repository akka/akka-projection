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
import akka.projection.HandlerRecovery
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.scaladsl.SourceProvider
import akka.projection.slick.internal.SlickProjectionImpl
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
      handler: SlickHandler[Envelope]): SlickProjection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.ExactlyOnce,
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
      saveOffsetAfterEnvelopes: Int,
      saveOffsetAfterDuration: FiniteDuration,
      handler: SlickHandler[Envelope]): SlickProjection[Envelope] =
    new SlickProjectionImpl(
      projectionId,
      sourceProvider,
      databaseConfig,
      SlickProjectionImpl.AtLeastOnce(saveOffsetAfterEnvelopes, saveOffsetAfterDuration),
      settingsOpt = None,
      handler)

}

trait SlickProjection[Envelope] extends Projection[Envelope] {

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit systemProvider: ClassicActorSystemProvider): Future[Done]
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
 * Error handling strategy for when processing an `Envelope` fails can be defined in the `Handler` by
 * overriding [[HandlerRecovery.onFailure]].
 */
@ApiMayChange
trait SlickHandler[Envelope] extends HandlerRecovery[Envelope] {

  def process(envelope: Envelope): DBIO[Done]

}
