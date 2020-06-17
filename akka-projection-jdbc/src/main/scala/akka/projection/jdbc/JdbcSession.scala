/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection
import java.sql.SQLException

import akka.japi.function.{ Function => JFunction }

trait JdbcSession {

  @throws(classOf[Exception])
  def withConnection[Result](func: JFunction[Connection, Result]): Result

  @throws(classOf[SQLException])
  def commit(): Unit

  @throws(classOf[SQLException])
  def rollback(): Unit

  @throws(classOf[SQLException])
  def close(): Unit
}
