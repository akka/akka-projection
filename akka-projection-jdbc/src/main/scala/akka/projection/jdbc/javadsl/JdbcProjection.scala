/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import scala.compat.java8.FutureConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.japi.function.Creator
import akka.japi.function.{ Function => JFunction }
import akka.projection.ProjectionId
import akka.projection.internal.ExactlyOnce
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SingleHandlerStrategy
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.ExactlyOnceProjection
import akka.projection.javadsl.HandlerLifecycle
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.internal.JdbcOffsetStore
import akka.projection.jdbc.internal.JdbcProjectionImpl
import akka.projection.jdbc.internal.JdbcSettings
import akka.projection.scaladsl.Handler

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
      handler: JdbcHandler[Envelope, S])(implicit system: ActorSystem[_]): ExactlyOnceProjection[Offset, Envelope] = {

    // internal API needs Creator adapted to Scala function
    val sessionFactory: () => S = sessionCreator.create _
    val offsetStore = createOffsetStore(sessionFactory)

    val adaptedHandler = new Handler[Envelope] {

      override def process(envelope: Envelope): Future[Done] = {
        val offset = sourceProvider.extractOffset(envelope)

        // this scope ensures that the blocking DB dispatcher is used solely for DB operations
        implicit val executionContext: ExecutionContext = offsetStore.executionContext

        JdbcSession
          .withSession(sessionFactory) { sess =>

            sess.withConnection[Unit] { conn =>
              offsetStore.saveOffsetBlocking(conn, projectionId, offset)
            }
            // run users handler
            handler.process(sess, envelope)
          }
          .map(_ => Done)
      }

      override def start(): Future[Done] = handler.start().toScala
      override def stop(): Future[Done] = handler.stop().toScala
    }

    new JdbcProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
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
      implicit system: ActorSystem[_]): CompletionStage[Done] =
    createOffsetStore(sessionFactory).createIfNotExists().toJava

  private def createOffsetStore[S <: JdbcSession](sessionFactory: () => S)(implicit system: ActorSystem[_]) =
    new JdbcOffsetStore[S](JdbcSettings(system), sessionFactory)
}

object JdbcSession {

  /**
   * INTERNAL API: run the blocking DB operations on a single thread on the passed ExecutionContext.
   */
  @InternalApi
  private[akka] def withSession[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(func: S => Result)(
      implicit ec: ExecutionContext): Future[Result] = {

    // all blocking calls here
    Future {
      val session = jdbcSessionFactory()
      try {
        val result = func(session)
        session.commit()
        result
      } catch {
        case NonFatal(ex) =>
          try {
            session.rollback()
          } catch {
            case NonFatal(_) => // the original exception is more interesting
          }
          throw ex
      } finally {
        try {
          session.close()
        } catch {
          case NonFatal(_) => // ignored
        }
      }
    }
  }

  private[akka] def withConnection[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(func: Connection => Result)(
      implicit ec: ExecutionContext): Future[Result] = {
    withSession(jdbcSessionFactory) { sess =>
      sess.withConnection { conn =>
        func(conn)
      }
    }
  }

  /**
   * INTERNAL API: try-with-resource. Mainly for internal usage with Statement and ResultSet
   *
   * The AutoCloseable is closed after usage. If an exception is thrown when closing it, it will be ignored.
   */
  @InternalApi
  private[akka] def tryWithResource[T, C <: AutoCloseable](closeable: => C)(func: C => T): T = {
    try {
      func(closeable)
    } finally {
      try {
        closeable.close()
      } catch {
        // if we get the result, but fail to close the statement, we just proceed
        // on connection close we will get another chance to close it
        case _: SQLException =>
      }
    }
  }
}

trait JdbcSession {
  def withConnection[Result](func: JFunction[Connection, Result]): Result

  def commit(): Unit
  def rollback(): Unit
  def close(): Unit
}

abstract class JdbcHandler[Envelope, S <: JdbcSession] extends HandlerLifecycle {
  def process(session: S, envelope: Envelope): Unit

  /**
   * Invoked when the projection is starting, before first envelope is processed.
   * Can be overridden to implement initialization. It is also called when the `Projection`
   * is restarted after a failure.
   */
  def start(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)

  /**
   * Invoked when the projection has been stopped. Can be overridden to implement resource
   * cleanup. It is also called when the `Projection` is restarted after a failure.
   */
  def stop(): CompletionStage[Done] =
    CompletableFuture.completedFuture(Done)
}
