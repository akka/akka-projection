/*
 * Copyright (C) 2021 Lightbend Inc. <https://www.lightbend.com>
 */

package akka.projection.r2dbc.scaladsl

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.internal.R2dbcExecutor
import io.r2dbc.spi.Connection
import io.r2dbc.spi.Row
import io.r2dbc.spi.Statement

final class R2dbcSession(val connection: Connection)(implicit val ec: ExecutionContext, val system: ActorSystem[_]) {

  def createStatement(sql: String): Statement =
    connection.createStatement(sql)

  def updateOne(statement: Statement): Future[Int] =
    R2dbcExecutor.updateOneInTx(statement)

  def update(statements: immutable.IndexedSeq[Statement]): Future[immutable.IndexedSeq[Int]] =
    R2dbcExecutor.updateInTx(statements)

  def selectOne[A](statement: Statement)(mapRow: Row => A): Future[Option[A]] =
    R2dbcExecutor.selectOneInTx(statement, mapRow)

  def select[A](statement: Statement)(mapRow: Row => A): Future[immutable.IndexedSeq[A]] =
    R2dbcExecutor.selectInTx(statement, mapRow)

}
