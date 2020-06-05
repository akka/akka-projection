/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.javadsl

import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.control.NonFatal

import akka.Done
import akka.actor.typed.ActorSystem
import akka.annotation.InternalApi
import akka.japi.function.{ Function => JFunction }
import akka.projection.Projection
import akka.projection.ProjectionId
import akka.projection.StatusObserver
import akka.projection.internal.NoopStatusObserver
import akka.projection.internal.SourceProviderAdapter
import akka.projection.javadsl.HandlerLifecycle
import akka.projection.javadsl.SourceProvider
import akka.projection.jdbc.internal.JdbcProjectionImpl

object JdbcProjection {

  def exactlyOnce[Offset, Envelope, S <: JdbcSession](
      projectionId: ProjectionId,
      sourceProvider: SourceProvider[Offset, Envelope],
      sessionFactory: () => S,
      handler: JdbcHandler[Envelope, S]): ExactlyOnceJdbcProjection[Envelope] =
    new JdbcProjectionImpl(
      projectionId,
      new SourceProviderAdapter(sourceProvider),
      sessionFactory,
      settingsOpt = None,
      restartBackoffOpt = None,
      handler,
      NoopStatusObserver)

}

trait JdbcProjection[Envelope] extends Projection[Envelope] {

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): JdbcProjection[Envelope]

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): JdbcProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): JdbcProjection[Envelope]

  /**
   * For testing purposes the offset table can be created programmatically.
   * For production it's recommended to create the table with DDL statements
   * before the system is started.
   */
  def createOffsetTableIfNotExists()(implicit system: ActorSystem[_]): CompletionStage[Done]
}

trait ExactlyOnceJdbcProjection[Envelope] extends JdbcProjection[Envelope] {

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double): ExactlyOnceJdbcProjection[Envelope]

  override def withRestartBackoff(
      minBackoff: FiniteDuration,
      maxBackoff: FiniteDuration,
      randomFactor: Double,
      maxRestarts: Int): ExactlyOnceJdbcProjection[Envelope]

  override def withStatusObserver(observer: StatusObserver[Envelope]): ExactlyOnceJdbcProjection[Envelope]

}

object JdbcSession {

  /**
   * INTERNAL API: run the blocking DB operations on a single thread on the passed ExecutionContext.
   */
  @InternalApi
  private[akka] def withSession[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(block: S => Result)(
      implicit ec: ExecutionContext): Future[Result] = {

    // all blocking calls here
    Future {
      val session = jdbcSessionFactory()
      try {
        val result = block(session)
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
        session.close()
      }
    }
  }

  private[akka] def withConnection[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(block: Connection => Result)(
      implicit ec: ExecutionContext): Future[Result] = {
    withSession(jdbcSessionFactory) { sess =>
      sess.withConnection { conn =>
        block(conn)
      }
    }
  }

  /**
   * INTERNAL API: try-with-resource for SQL [[Statement]].
   *
   * The Statement is closed after usage. If an exception is thrown when closing the Statement,
   * it will be ignored and the resources will be (hopefully) released when the [[Connection]] is closed.
   */
  @InternalApi
  private[akka] def tryWithResource[T, S <: Statement](stmt: => S)(func: S => T): T = {
    try {
      func(stmt)
    } finally {
      try {
        stmt.close()
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
