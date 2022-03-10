/*
 * Copyright (C) 2020-2022 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc.internal

import java.sql.Connection
import java.sql.SQLException

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.control.NonFatal

import akka.annotation.InternalApi
import akka.projection.jdbc.JdbcSession

/**
 * INTERNAL API
 */
@InternalApi
private[projection] object JdbcSessionUtil {

  /**
   * run the blocking DB operations on a single thread on the passed ExecutionContext.
   */
  def withSession[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(func: S => Result)(
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

  def withConnection[S <: JdbcSession, Result](jdbcSessionFactory: () => S)(func: Connection => Result)(
      implicit ec: ExecutionContext): Future[Result] = {
    withSession(jdbcSessionFactory) { sess =>
      sess.withConnection { conn =>
        func(conn)
      }
    }
  }

  /**
   * try-with-resource. Mainly for internal usage with Statement and ResultSet
   *
   * The AutoCloseable is closed after usage. If an exception is thrown when closing it, it will be ignored.
   */
  def tryWithResource[T, C <: AutoCloseable](closeable: => C)(func: C => T): T = {
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
