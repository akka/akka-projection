/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.scaladsl

import scala.concurrent.Future

import akka.Done
import akka.actor.typed.ActorSystem
import akka.projection.ProjectionId
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.JdbcProjectionImpl
import akka.projection.scaladsl.ExactlyOnceProjection
import akka.projection.scaladsl.SourceProvider

object JdbcProjection {

  /**
   * Create a [[akka.projection.Projection]] with exactly-once processing semantics.
   *
   * It stores the offset in a relational database table using JDBC in the same transaction
   * as the user defined `handler`.
   *
   */
  def exactlyOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handler: () => JdbcHandler[Envelope, S])(
      implicit system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForExactlyOnce(
        projectionId,
        sourceProvider,
        sessionFactory,
        handler,
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      sourceProvider,
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      ExactlyOnce(),
      SingleHandlerStrategy(adaptedHandler),
      NoopStatusObserver,
      offsetStore)
  }

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists[S <: JdbcSession](sessionFactory: () => S)(
      implicit system: ActorSystem[_]): Future[Done] =
    JdbcProjectionImpl.createOffsetStore(sessionFactory).createIfNotExists()

}
