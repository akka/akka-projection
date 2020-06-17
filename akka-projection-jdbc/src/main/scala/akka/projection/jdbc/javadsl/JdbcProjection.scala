/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._

import akka.Done
import akka.actor.typed.ActorSystem
import akka.japi.function.Creator
import akka.projection.ProjectionId
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.JdbcSession
import akka.projection.jdbc.internal.JdbcHandlerAdapter
import akka.projection.jdbc.internal.JdbcProjectionImpl

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
      sessionCreator: Creator[S],
      handler: JdbcHandler[Envelope, S],
      system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    val sessionFactory = () => sessionCreator.create()
    val javaSourceProvider = new SourceProviderAdapter(sourceProvider)
    val offsetStore = JdbcProjectionImpl.createOffsetStore(sessionFactory)(system)

    val adaptedHandler =
      JdbcProjectionImpl.adaptedHandlerForExactlyOnce(
        projectionId,
        javaSourceProvider,
        sessionFactory,
        new JdbcHandlerAdapter(handler),
        offsetStore)

    new JdbcProjectionImpl(
      projectionId,
      javaSourceProvider,
      sessionFactory = sessionFactory,
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
  def createOffsetTableIfNotExists[S <: JdbcSession](
      sessionFactory: () => S,
      system: ActorSystem[_]): CompletionStage[Done] =
    JdbcProjectionImpl.createOffsetStore(sessionFactory)(system).createIfNotExists().toJava

}
