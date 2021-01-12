/*
 * Copyright (C) 2020-2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.SQLException

import akka.annotation.ApiMayChange
import akka.japi.function.{ Function => JFunction }

/**
 * Generic interface to give access to basic JDBC connection operations.
 *
 * A new [[JdbcSession]] will be created for each call to the projection handler. Upon creation a database connection
 * must be created (preferably relying on connection pool) and used through out the live of this instance.
 *
 * After usage, the underlying database connection will be closed (returned to the pool) after committing or rolling back
 * the transaction. It's paramount to configure the connection to NOT use `auto-commit` in order to guarantee that the
 * event handling and offset persistence operations participate on the same transaction.
 *
 * The only requirement to implement a [[JdbcSession]] is to have access to the underlying JDBC [[Connection]].
 * When using plain JDBC, one can initialize a connection directly, but when relying on a JDBC framework like JPA it will depend on the
 * chosen implementation. Hibernate for instance provides indirect access to the underlying connection through a
 * lambda call and therefore can be used (see [[JdbcSession#withConnection]] method). Other JPA implementations may not provide this feature.
 *
 */
@ApiMayChange
trait JdbcSession {

  /**
   * This method provides access to the underlying connection through a lambda call.
   * Implementors should ensure that every single call to this method instance uses the same JDBC connection instance.
   *
   * For plain JDBC implementations an instance of the open connection should be kept as internal state and passed to the lambda call.
   * For implementations based on Hibernate, this method can be rely on Hibernate's  `Session.doReturningWork`.
   */
  @throws(classOf[Exception])
  def withConnection[Result](func: JFunction[Connection, Result]): Result

  /**
   * Commits the transaction after processing.
   * Should delegate to [[Connection#commit()]] or equivalent depending on underlying JDBC framework.
   *
   * @throws java.sql.SQLException
   */
  @throws(classOf[SQLException])
  def commit(): Unit

  /**
   * Rollback the transaction in case of failures.
   * Should delegate to [[Connection#rollback()]] or equivalent depending on underlying JDBC framework.
   *
   * @throws java.sql.SQLException
   */
  @throws(classOf[SQLException])
  def rollback(): Unit

  /**
   * Closes the connection after use.
   * Should delegate to [[Connection#close()]] or equivalent depending on underlying JDBC framework.
   *
   * @throws java.sql.SQLException
   */
  @throws(classOf[SQLException])
  def close(): Unit
}
