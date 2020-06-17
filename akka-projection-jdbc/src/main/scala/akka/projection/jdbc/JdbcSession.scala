/*
 * Copyright (C) 2020 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.jdbc

import java.sql.Connection

import akka.japi.function.{ Function => JFunction }

trait JdbcSession {
  def withConnection[Result](func: JFunction[Connection, Result]): Result

  def commit(): Unit
  def rollback(): Unit
  def close(): Unit
}
